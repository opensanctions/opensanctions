import { CaretRightFill, EnvelopeFill, Github, Slack, Twitter } from 'react-bootstrap-icons';
import Button from 'react-bootstrap/Button';
import Card from 'react-bootstrap/Card';
import CardGroup from 'react-bootstrap/CardGroup';

import Layout from '../components/Layout'
import Content from '../components/Content'
import { Summary } from '../components/util';
import Link from 'next/link';

export default function Contact() {
  const title = "Contact the team";
  const summary = "OpenSanctions exists to start a conversation. We're keen to get in touch with anyone who is interested in using the data, giving us feedback, or supporting to the project.";
  return (
    <Layout.Base title={title} description={summary} >
      <Content.Menu title={title}>
        <Summary summary={summary} />
        <div className="text-body">
          <CardGroup className="actions">
            <Card bg="primary" text="white">
              <Card.Body>
                <Card.Title><EnvelopeFill className="bsIcon" /> E-mail</Card.Title>
                <Card.Text>
                  Reach out to the core team to discuss the project.
                </Card.Text>
              </Card.Body>
              <Card.Footer>
                <Button href="mailto:info@opensanctions.org" variant="light">
                  <CaretRightFill className="bsIcon" /> info@opensanctions.org
                </Button>
              </Card.Footer>
            </Card>
            <Card bg="secondary" text="white">
              <Card.Body>
                <Card.Title><Slack /> Slack chat</Card.Title>
                <Card.Text>
                  Join our Slack workspace and chat with the team and others
                  working on sanctions data.
                </Card.Text>
              </Card.Body>
              <Card.Footer>
                <Button href="https://bit.ly/osa-slack" variant="light">
                  <CaretRightFill className="bsIcon" /> OpenSanctions Slack
                </Button>
              </Card.Footer>
            </Card>
            <Card bg="secondary" text="white">
              <Card.Body>
                <Card.Title><Twitter /> Twitter</Card.Title>
                <Card.Text>
                  Keep in the loop with OpenSanctions on Twitter.
                </Card.Text>
              </Card.Body>
              <Card.Footer>
                <Button href="https://twitter.com/open_sanctions" variant="light">
                  <CaretRightFill className="bsIcon" /> @open_sanctions
                </Button>
              </Card.Footer>
            </Card>
          </CardGroup>
          <p>
            Also check out the <Link href="/docs/">documentation</Link> and
            the <Link href="/docs/faq">frequently asked questions</Link> for
            detailed information regarding the data and project.
            And, of course, please consider communicating with us
            in <Link href="/sponsor">the sweetest language of all</Link>.
          </p>
        </div>
      </Content.Menu>
    </Layout.Base >
  )
}
