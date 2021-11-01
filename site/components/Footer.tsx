import React from 'react';
import Link from 'next/link';
import { Twitter, HeartFill, EnvelopeFill, Slack } from 'react-bootstrap-icons';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Container from 'react-bootstrap/Container';

import styles from '../styles/Footer.module.scss';
import { LICENSE_URL, SPACER } from '../lib/constants';

export default class Footer extends React.Component {
  render() {
    return (
      <div className={styles.footer}>
        <Container>
          <Row>
            <Col md={3}>
              <img
                src="/static/footer.png"
                alt="OpenSanctions project"
                className={styles.logo}
              />
            </Col>
            <Col md={9}>
              <Row>
                <Col md={4}>
                  <strong>Project</strong>
                  <ul>
                    <li>
                      <Link href="/docs/about/">About OpenSanctions</Link>
                    </li>
                    <li>
                      <Link href="/docs/">Documentation</Link>
                    </li>
                    <li>
                      <Link href="/sponsor/">Sponsor the project</Link>
                    </li>
                  </ul>
                </Col>
                <Col md={4}>
                  <strong>Collections</strong>
                  <ul>
                    <li>
                      <Link href="/datasets/sanctions/">Consolidated global sanctions</Link>
                    </li>
                    <li>
                      <Link href="/datasets/peps/">Politically exposed persons</Link>
                    </li>
                    <li>
                      <Link href="/datasets/crime/">Criminal watchlists</Link>
                    </li>
                  </ul>
                </Col>
                <Col md={4}>
                  <strong>Keep in touch</strong>
                  <ul>
                    <li>
                      <Link href="https://bit.ly/osa-slack"><Slack /></Link>
                      {' '}
                      <Link href="https://bit.ly/osa-slack">Slack chat</Link>
                    </li>
                    <li>
                      <Link href="https://twitter.com/open_sanctions"><Twitter /></Link>
                      {' '}
                      <Link href="https://twitter.com/open_sanctions">Twitter</Link>
                    </li>
                    <li>
                      <Link href="/contact/"><EnvelopeFill /></Link>
                      {' '}
                      <Link href="/contact/">Contact us</Link>
                    </li>
                  </ul>
                </Col>
              </Row>
              <Row>
                <p className={styles.copyright}>
                  The content and data published on this site are licensed under
                  the terms of <Link href={LICENSE_URL}>Creative Commons Attribution 4.0</Link>.
                </p>
                <p className={styles.copyright}>
                  Made with <HeartFill className={styles.love} /> in Python
                  {SPACER}
                  <Link href="/impressum/">Impressum</Link>
                </p>
              </Row>
            </Col>
          </Row>
        </Container >
      </div >
    )
  }
}