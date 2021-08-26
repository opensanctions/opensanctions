import { ChatQuoteFill, CurrencyExchange, EnvelopeFill, Github, Heart, HeartFill } from 'react-bootstrap-icons';
import Button from 'react-bootstrap/Button';
import Card from 'react-bootstrap/Card';
import CardGroup from 'react-bootstrap/CardGroup';

import Layout from '../components/Layout'
import Content from '../components/Content'
import { Summary } from '../components/util';

export default function Sponsor() {
  const title = "Help us to make OpenSanctions sustainable!";
  const summary = "We're soliciting recurring sponsorships from commercial users to ensure the long-term maintenance of OpenSanctions.";
  return (
    <Layout.Base title="Sponsor the project" description={summary}>
      <Content.Menu title={title}>
        <Summary summary={summary} />
        <div className="text-body">
          <p>
            OpenSanctions provides free, high quality data for sanctions list screening.
            Maintaining that database is a continuous effort. The project will be most
            valuable if its long-term operation and maintenance is guaranteed. That’s why
            we are inviting commercial users of OpenSanctions to establish a recurring
            sponsorship.
          </p>
          <CardGroup className="actions">
            <Card bg="primary" text="white">
              <Card.Body>
                <Card.Title><Github /> GitHub Sponsors</Card.Title>
                <Card.Text>
                  GitHub's sponsorship program matches your contributions.
                </Card.Text>
                <Card.Text>
                  <small>Requires a free user account.</small>
                </Card.Text>
              </Card.Body>
              <Card.Footer>
                <Button href="https://github.com/sponsors/pudo" variant="light">
                  <HeartFill /> Become a sponsor
                </Button>
              </Card.Footer>
            </Card>
            <Card bg="secondary" text="white">
              <Card.Body>
                <Card.Title><CurrencyExchange className="bsIcon" /> OpenCollective</Card.Title>
                <Card.Text>
                  A service that helps open source projects collect donations.
                </Card.Text>
                <Card.Text>
                  <small>No sign-up required.</small>
                </Card.Text>
              </Card.Body>
              <Card.Footer>
                <Button href="https://opencollective.com/opensanctions" variant="light">
                  <HeartFill /> Contribute
                </Button>
              </Card.Footer>
            </Card>
            <Card bg="secondary" text="white">
              <Card.Body>
                <Card.Title><ChatQuoteFill className="bsIcon" /> Contact us</Card.Title>
                <Card.Text>
                  We're keen discuss a different type of partnership.
                </Card.Text>
                <Card.Text>
                  <small>Service contract/invoice.</small>
                </Card.Text>
              </Card.Body>
              <Card.Footer>
                <Button href="/contact/" variant="light">
                  <EnvelopeFill /> Get in touch
                </Button>
              </Card.Footer>
            </Card>
          </CardGroup>
          <p>
            Besides ensuring the continuity of the project, sponsors will receive the
            following benefits:
          </p>
          <ul>
            <li>
              <strong>Active support</strong> from the developer team when it comes to
              using the data and generating custom exports.
            </li>
            <li>
              <strong>Prioritised implementation</strong> of additional
              data sources requested by sponsors.
            </li>
          </ul>
          <p>
            Continued operation of the OpenSanctions data platform is guaranteed
            until <strong>January 31st, 2022</strong>. That’s when we’ll need
            to decide if we’ve managed to build a sustainable level of sponsorship to
            extend the system’s operation indefinitely.
          </p>
          <p>
            <small>
              Please note that OpenSanctions is not a registered non-profit
              organization. We will not be able to issue a donation receipt
              for tax purposes.
            </small>
          </p>
        </div>
      </Content.Menu>
    </Layout.Base >
  )
}
