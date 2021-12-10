import Link from 'next/link';
import Alert from 'react-bootstrap/Alert';

import styles from '../styles/Policy.module.scss';

export function LicenseInfo() {
  return (
    <Alert variant="light" className={styles.licenseBox}>
      OpenSanctions is <strong>free for non-commercial users.</strong> Business users
      need to <Link href="/licensing">acquire a license</Link> to support the
      continued development and operation of the project.
    </Alert>
  );
}