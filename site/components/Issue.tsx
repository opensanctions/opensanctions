import Table from "react-bootstrap/Table";
import { IIssue } from "../lib/dataset";

import styles from '../styles/Issue.module.scss'

type IssueRowProps = {
  issue: IIssue
}

function IssueRow({ issue }: IssueRowProps) {
  return (
    <tr>
      <td width="15%">
        <code className={styles.code}>{issue.module}</code>
      </td>
      <td>
        <Table size="sm" hover className={styles.dataTable}>
          <tbody>
            <tr key='message'>
              <th>
                <code className={styles.code}>message</code>
              </th>
              <td>
                <code className={styles.code}>{issue.message}</code>
              </td>
            </tr>
            {Object.keys(issue.data).map((key) => (
              <tr key={key}>
                <th className={styles.tableKey}>
                  <code className={styles.code}>{key}</code>
                </th>
                <td className={styles.tableValue}>
                  <code className={styles.code}>{issue.data[key]}</code>
                </td>
              </tr>
            ))}
          </tbody>
        </Table>
      </td>
    </tr>
  )
}


type IssuesTableProps = {
  issues: Array<IIssue>
}

export default function IssuesTable({ issues }: IssuesTableProps) {
  return (
    <Table>
      <thead>
        <tr>
          <th>Module</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        {issues.map((issue) => <IssueRow key={issue.id} issue={issue} />)}
      </tbody>
    </Table>
  )
}
