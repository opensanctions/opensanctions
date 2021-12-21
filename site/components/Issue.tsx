import Link from 'next/link';
import Badge from "react-bootstrap/Badge";
import Table from "react-bootstrap/Table";
import Card from "react-bootstrap/Card";

import styles from '../styles/Issue.module.scss'
import { SPACER } from "../lib/constants";
import { IIssue } from "../lib/types";
import { FormattedDate, Spacer } from "./util";

type IssueProps = {
  issue: IIssue
  showDataset: boolean
}

type IssuesListProps = {
  issues: Array<IIssue>
  showDataset: boolean
}

function getDataValue(value: any): string {
  if (value === undefined || value === null) {
    return '-'
  }
  if (typeof value === 'string') {
    return value;
  }
  return JSON.stringify(value)
}

function IssueCard({ issue, showDataset }: IssueProps) {
  const accentColor = issue.level == 'error' ? 'danger' : 'warning';
  const datasetLink = <Link href={`/datasets/${issue.dataset}`}>{issue.dataset}</Link>;
  const headerContent = (!showDataset) ?
    <code>Issue #{issue.id}: {issue.module}</code> :
    <><code>Issue #{issue.id} [{datasetLink}]: {issue.module}</code></>
  return (
    <Card key={issue.id} className={styles.issueCard} border={accentColor}>
      <Card.Header className={styles.issueHeader}>{headerContent}</Card.Header>
      <Card.Body>{issue.message}</Card.Body>
      <Table bordered>
        <tbody>
          {Object.keys(issue.data).map((key) => (
            <tr key={key}>
              <th className={styles.issueTableKey}>
                {key}
              </th>
              <td>
                <code className={styles.issueValue}>{getDataValue(issue.data[key])}</code>
              </td>
            </tr>
          ))}
        </tbody>
      </Table>
      <Card.Footer className={styles.issueFooter}>
        <Badge bg={accentColor}>{issue.level}</Badge>
        <Spacer />
        <FormattedDate date={issue.timestamp} />
      </Card.Footer>
    </Card>
  )
}


export function IssuesList({ issues, showDataset }: IssuesListProps) {
  return <>{issues.map((iu) => <IssueCard key={iu.id} issue={iu} showDataset={showDataset} />)}</>;
}