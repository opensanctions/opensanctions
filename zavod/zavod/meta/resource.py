import mimetypes
from hashlib import sha1
from pathlib import Path
from typing import Optional, Dict, Any
from nomenklatura.dataset import DataResource as NKDataResource

from zavod.meta.dataset import Dataset
from zavod.archive import dataset_path


class DataResource(NKDataResource):
    @classmethod
    def from_path(
        cls,
        dataset: Dataset,
        path: Path,
        mime_type: Optional[str] = None,
        title: Optional[str] = None,
    ) -> "DataResource":
        """Create a resource description object from a local file path."""
        if not path.exists():
            raise ValueError("File does not exist: %s" % path)
        if mime_type is None:
            mime_type, _ = mimetypes.guess_type(path.as_posix(), strict=False)
        dataset_path_ = dataset_path(dataset.name)
        name = path.relative_to(dataset_path_).as_posix()

        digest = sha1()
        size = 0
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(65536)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
        if size == 0:
            raise ValueError("Resource is empty: %s" % name)
        checksum = digest.hexdigest()
        data = {
            "name": name,
            "title": title,
            "checksum": checksum,
            "mime_type": mime_type,
            "size": size,
            "url": dataset.make_public_url(name),
        }
        return cls(data)

    def to_opensanctions_dict(self) -> Dict[str, Any]:
        """Convert this resource description to a backward-compatible OpenSanctions
        resource description."""
        data = self.to_dict()
        data["path"] = data["name"]
        return data
