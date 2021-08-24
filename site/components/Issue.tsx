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


// type IssueSectionProps = {
//   index: IIssueIndex
// }

// export default function IssueSection({ index }: IssueSectionProps) {
//   if (index.issues.length === 0) {
//     return null;
//   }

//   return (
//     <>
//       <h2>Data issues</h2>
//       <p>
//         As an open source project, one of our values is transparency. That's
//         why below we list all known inconsistencies that have occurred in the
//         process of importing this dataset.
//       </p>
//       {errors.length > 0 && (
//         <>
//           <h3><a id="errors"></a> Errors</h3>
//           <IssuesTable issues={errors} />
//         </>
//       )}
//       {warnings.length > 0 && (
//         <>
//           <h3><a id="warnings"></a> Warnings</h3>
//           <IssuesTable issues={warnings} />
//         </>
//       )}
//     </>
//   )
// }