from typing import List
import ujson
import click
import logging
import numpy as np
from pprint import pprint
from followthemoney import model
from followthemoney.dedupe import Judgement
from nomenklatura.matching.pairs import JudgedPair
from nomenklatura.matching.predicates import PREDICATES
from opensanctions.core import Entity, configure_logging

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics


log = logging.getLogger("matching-train")


def apply_predicates(pair: JudgedPair):
    scores = {}
    for func in PREDICATES:
        scores[func.__name__] = func(pair.left, pair.right)
    return scores


def pairs_to_arrays(pairs: List[JudgedPair]):
    xrows = []
    yrows = []
    for pair in pairs:
        preds = apply_predicates(pair)
        xvals = list(preds.values())
        xrows.append(xvals)

        yval = 0
        if pair.judgement == Judgement.POSITIVE:
            yval = 1
        yrows.append(yval)
    return np.array(xrows), np.array(yrows)


def read_pairs(pairs_file):
    pairs = []
    with open(pairs_file, "r") as fh:
        while line := fh.readline():
            data = ujson.loads(line)
            left_entity = Entity.from_dict(model, data["left"])
            right_entity = Entity.from_dict(model, data["right"])
            judgement = Judgement(data["judgement"])
            if judgement not in (Judgement.POSITIVE, Judgement.NEGATIVE):
                continue
            pair = JudgedPair[Entity](left_entity, right_entity, judgement)
            pairs.append(pair)
    return pairs


@click.command()
@click.argument("pairs_file", type=click.Path(exists=True, file_okay=True))
def train_matcher(pairs_file):
    pairs = read_pairs(pairs_file)
    print("total pairs", len(pairs))
    print("positive", len([p for p in pairs if p.judgement == Judgement.POSITIVE]))
    print("negative", len([p for p in pairs if p.judgement == Judgement.NEGATIVE]))
    X, y = pairs_to_arrays(pairs)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33)
    print("built training data")
    # based on: https://www.datacamp.com/community/tutorials/understanding-logistic-regression-python
    logreg = LogisticRegression()
    logreg.fit(X_train, y_train)
    coef = logreg.coef_[0]
    coef_named = {n.__name__: c for n, c in zip(PREDICATES, coef)}
    print("Coefficients:")
    pprint(coef_named)

    y_pred = logreg.predict(X_test)
    cnf_matrix = metrics.confusion_matrix(y_test, y_pred)
    print("Confusion matrix:\n", cnf_matrix)
    print("Accuracy:", metrics.accuracy_score(y_test, y_pred))
    print("Precision:", metrics.precision_score(y_test, y_pred))
    print("Recall:", metrics.recall_score(y_test, y_pred))

    y_pred_proba = logreg.predict_proba(X_test)[::, 1]
    auc = metrics.roc_auc_score(y_test, y_pred_proba)
    print("AUC:", auc)


if __name__ == "__main__":
    configure_logging()
    train_matcher()
