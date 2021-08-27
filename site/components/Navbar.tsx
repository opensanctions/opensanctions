import React from 'react';
import Link from 'next/link';
import { useRouter } from 'next/router';
import { PatchCheckFill } from 'react-bootstrap-icons';
import Navbar from 'react-bootstrap/Navbar';
import Nav from 'react-bootstrap/Nav';
import Button from 'react-bootstrap/Button';
import Container from 'react-bootstrap/Container';

import styles from '../styles/Navbar.module.scss';

export default function NavbarSection() {
  const activePath = useRouter().asPath;
  const inDataset = activePath.startsWith('/datasets/');
  const inAbout = activePath === '/docs/about/';
  const inFaq = activePath === '/docs/faq/';
  return (
    <Navbar bg="light" expand="lg">
      <Container>
        <Link href="/" passHref>
          <Navbar.Brand href="#home">
            <img
              src="/static/navlogo.webp"
              width="30"
              height="30"
              className="align-top"
              alt="OpenSanctions"
            />
          </Navbar.Brand>
        </Link>
        <Link href="/" passHref>
          <Navbar.Brand className={styles.brand}>
            OpenSanctions
          </Navbar.Brand>
        </Link>
        <Navbar.Toggle />
        <Navbar.Collapse>
          <Nav className="justify-content-end">
            <Link href="/datasets/" passHref>
              <Nav.Link className={styles.navItem} active={inDataset}>Datasets</Nav.Link>
            </Link>
            <Link href="/docs/about/" passHref>
              <Nav.Link className={styles.navItem} active={inAbout}>About</Nav.Link>
            </Link>
            <Link href="/docs/faq/" passHref>
              <Nav.Link className={styles.navItem} active={inFaq}>FAQ</Nav.Link>
            </Link>
          </Nav>
        </Navbar.Collapse>
        <Navbar.Collapse className="justify-content-end">
          <Nav className="justify-content-end">
            <Link href="/sponsor/" passHref>
              <Button variant="primary" className={styles.sponsorCall}>
                <PatchCheckFill className={styles.sponsorIcon} />
                Sponsor
              </Button>
            </Link>
          </Nav>
        </Navbar.Collapse>
      </Container>
    </Navbar >
  )
}