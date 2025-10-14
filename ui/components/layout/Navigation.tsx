import classNames from 'classnames';
import Link from 'next/link';
import React from 'react';
import { NavbarBrand, NavbarCollapse, Navbar, Nav, Container, NavLink } from 'react-bootstrap';

import styles from '@/styles/Navigation.module.scss';

export default function Navigation({}) {
  return (
    <Navbar expand="lg" className={styles.navBar} role="banner">
      <Container>
        <Link href="/" passHref>
          <NavbarBrand>
            <img
              src="https://assets.opensanctions.org/images/nura/logo-oneline-color.svg"
              width="190"
              height="30"
              className="align-top"
              alt="OpenSanctions"
            />
          </NavbarBrand>
        </Link>
        <NavbarCollapse className="justify-content-end">
          <Nav
            className={classNames("justify-content-end", styles.nav)}
            variant="pills"
            role="navigation"
            aria-label="Site menu"
          >
            <Link href="/positions/" className={classNames("nav-link", styles.navItem)}>Positions</Link>
            <Link href="/review/" className={classNames("nav-link", styles.navItem)}>Reviews</Link>
          </Nav>
        </NavbarCollapse>
      </Container>
    </Navbar >
  )
}