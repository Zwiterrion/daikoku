import React, { Component, useEffect } from 'react';
import { Link, Route } from 'react-router-dom';
import classNames from 'classnames';
import { connect } from 'react-redux';

import * as Services from '../../services';
import {
  Error,
  Can,
  manage,
  read,
  api,
  apikey,
  stat,
  team,
  asset,
  daikoku,
  tenant as TENANT,
} from '../utils';
import { t, Translation } from '../../locales';

function elvis(value, f) {
  if (value) {
    return f(value);
  } else {
    return value;
  }
}

const BackOfficeContent = (props) => {
  return (
    <div className="pt-4 pr-3" style={{ height: '100%' }}>
      {props.error.status && <Error error={props.error} />}
      {!props.error.status && props.children}
    </div>
  );
};

class TeamBackOfficeHomeComponent extends Component {
  state = {
    team: undefined,
  };

  componentDidMount() {
    this.props.history.listen(() => {
      elvis(document.getElementById('sidebar'), (e) =>
        e.setAttribute('class', 'col-md-2 d-md-block sidebar collapse')
      );
      // document.getElementById('navbar').setAttribute('class', 'navbar-collapse collapse');
      elvis(document.getElementById('toggle-sidebar'), (e) =>
        e.setAttribute('class', 'navbar-toggle menu collapsed')
      );
      // document.getElementById('toggle-navigation').setAttribute('class', 'navbar-toggle collapsed');
    });

    Services.teamHome(this.props.currentTeam._id).then((team) => this.setState({ team }));
  }

  render() {
    if (!this.state.team) {
      return null;
    }

    return (
      <TeamBackOffice tab="Home" title={`${this.props.currentTeam.name}`}>
        <div className="row">
          <div className="col">
            <h1>
              {this.props.currentTeam.name}
              <a
                className="ml-1 btn btn-sm btn-access-negative"
                title="View this Team"
                href={`/${this.props.currentTeam._humanReadableId}`}>
                <i className="fas fa-eye"></i>
              </a>
            </h1>
            <div className="d-flex justify-content-center align-items-center col-12 mt-5">
              <div className="home-tiles d-flex justify-content-center align-items-center flex-wrap">
                <Link
                  to={`/${this.props.currentTeam._humanReadableId}/settings/apis`}
                  className="home-tile">
                  <span className="home-tile-number">{this.state.team.apisCount}</span>
                  <span className="home-tile-text">
                    <Translation
                      i18nkey="apis published"
                      language={this.props.currentLanguage}
                      count={this.state.team.apisCount}>
                      apis published
                    </Translation>
                  </span>
                </Link>
                <Link
                  to={`/${this.props.currentTeam._humanReadableId}/settings/apikeys`}
                  className="home-tile">
                  <span className="home-tile-number">{this.state.team.subscriptionsCount}</span>
                  <span className="home-tile-text">
                    <Translation
                      i18nkey="apis subcriptions"
                      language={this.props.currentLanguage}
                      count={this.state.team.subscriptionsCount}>
                      apis subcriptions
                    </Translation>
                  </span>
                </Link>
                <Link
                  to={
                    this.props.currentTeam.type === 'Personal'
                      ? '#'
                      : `/${this.props.currentTeam._humanReadableId}/settings/members`
                  }
                  className="home-tile"
                  disabled={this.props.currentTeam.type === 'Personal' ? 'disabled' : null}>
                  {this.props.currentTeam.type !== 'Personal' && (
                    <>
                      <span className="home-tile-number">{this.state.team.users.length}</span>
                      <span className="home-tile-text">
                        <Translation
                          i18nkey="members"
                          language={this.props.currentLanguage}
                          count={this.state.team.users.length}>
                          members
                        </Translation>
                      </span>
                    </>
                  )}
                </Link>
                <Link to={'/notifications'} className="home-tile">
                  <span className="home-tile-number">{this.state.team.notificationCount}</span>
                  <span className="home-tile-text">
                    <Translation
                      i18nkey="unread notifications"
                      language={this.props.currentLanguage}
                      count={this.state.team.notificationCount}>
                      unread notifications
                    </Translation>
                  </span>
                </Link>
              </div>
            </div>
          </div>
        </div>
      </TeamBackOffice>
    );
  }
}

const TeamBackOfficeComponent = ({
  tab,
  currentTeam,
  currentLanguage,
  tenant,
  isLoading,
  error,
  title,
  children,
}) => {
  // UNSAFE_componentWillMount() {
  //   if (!this.props.currentTeam || (this.props.currentTeam && !this.props.currentTeam._id)) {
  //     console.warn(
  //       'The <TeamBackOffice /> component does not have a team id props. Everything will fail !'
  //     );
  //   }
  // }

  // __componentWillReceiveProps(nextProps) {
  //   if (
  //     this.props.currentTeam &&
  //     nextProps.team &&
  //     nextProps.team._id !== this.props.currentTeam._id
  //   ) {
  //     console.log('force');
  //     this.forceUpdate();
  //   }
  // }

  useEffect(() => {
    if (title) {
      document.title = title;
    }
  }, [title]);

  if (!currentTeam) {
    return null;
  }

  return (
    <>
      <Route
        path="/:teamId/settings"
        render={() => (
          <div className="row">
            <button
              id="toggle-sidebar"
              type="button"
              className="navbar-toggle btn btn-sm btn-access-negative float-left mr-2"
              data-toggle="collapse"
              data-target="#sidebar"
              aria-expanded="false"
              aria-controls="sidebar">
              <span className="sr-only">Toggle sidebar</span>
              <span className="chevron" />
            </button>
            <nav className="col-md-2 d-md-block sidebar collapse" id="sidebar">
              <div className="sidebar-sticky">
                <h6 className="sidebar-heading d-flex justify-content-between align-items-center px-3 mt-4 mb-1 text-muted">
                  <Link to={`/${currentTeam._humanReadableId}/settings`}>{currentTeam.name}</Link>
                  {currentTeam.type === 'Organization' && (
                    <Can I={manage} a={team} team={currentTeam}>
                      <Link
                        to={`/${currentTeam._humanReadableId}/settings/edition`}
                        className=""
                        title={t('Update team', currentLanguage)}>
                        <i className="fas fa-pen" />
                      </Link>
                    </Can>
                  )}
                </h6>
                <ul className="nav flex-column mt-3">
                  {(!tenant.creationSecurity || currentTeam.apisCreationPermission) && (
                    <Can I={read} a={api} team={currentTeam}>
                      <li className="nav-item">
                        <Link
                          className={`nav-link ${tab === 'Apis' ? 'active' : ''}`}
                          to={`/${currentTeam._humanReadableId}/settings/apis`}>
                          <i className="fas fa-atlas" />
                          <Translation i18nkey="Team Apis" language={currentLanguage}>
                            Team Apis
                          </Translation>
                        </Link>
                      </li>
                    </Can>
                  )}
                  {(!tenant.creationSecurity || currentTeam.apisCreationPermission) && (
                    <Can I={read} a={api} team={currentTeam}>
                      <li className="nav-item">
                        <Link
                          className={`nav-link ${tab === 'Income' ? 'active' : ''}`}
                          to={`/${currentTeam._humanReadableId}/settings/income`}>
                          <i className="fas fa-file-invoice-dollar" />
                          <Translation i18nkey="Team Income" language={currentLanguage}>
                            Team Income
                          </Translation>
                        </Link>
                      </li>
                    </Can>
                  )}
                  <Can I={read} a={apikey} team={currentTeam}>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'ApiKeys' ? 'active' : ''}`}
                        to={`/${currentTeam._humanReadableId}/settings/apikeys`}>
                        <i className="fas fa-key" />
                        <Translation i18nkey="Team api keys" language={currentLanguage}>
                          Team api keys
                        </Translation>
                      </Link>
                    </li>
                  </Can>
                  <Can I={read} a={stat} team={currentTeam}>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Billing' ? 'active' : ''}`}
                        to={`/${currentTeam._humanReadableId}/settings/billing`}>
                        <i className="fas fa-file-invoice-dollar" />
                        <Translation i18nkey="Team billing" language={currentLanguage}>
                          Team billing
                        </Translation>
                      </Link>
                    </li>
                  </Can>

                  {currentTeam.type === 'Organisation' && (
                    <Can I={manage} a={team} team={currentTeam}>
                      <li className="nav-item">
                        <Link
                          className={`nav-link ${tab === 'Members' ? 'active' : ''}`}
                          to={`/${currentTeam._humanReadableId}/settings/members`}>
                          <i className="fas fa-users" />
                          <Translation i18nkey="Team members" language={currentLanguage}>
                            Team members
                          </Translation>
                        </Link>
                      </li>
                    </Can>
                  )}
                  {currentTeam.type !== 'Admin' && (
                    <Can I={manage} a={asset} team={currentTeam}>
                      <li className="nav-item">
                        <Link
                          className={`nav-link ${tab === 'Assets' ? 'active' : ''}`}
                          to={`/${currentTeam._humanReadableId}/settings/assets`}>
                          <i className="fas fa-tools" />
                          <Translation i18nkey="Team assets" language={currentLanguage}>
                            Team assets
                          </Translation>
                        </Link>
                      </li>
                    </Can>
                  )}
                </ul>
              </div>
            </nav>
            <main role="main" className="col-md-10 ml-sm-auto px-4">
              <div
                className={classNames('back-office-overlay', {
                  active: isLoading && !error.status,
                })}
              />
              <BackOfficeContent error={error}>{children}</BackOfficeContent>
            </main>
          </div>
        )}
      />
    </>
  );
};

const UserBackOfficeComponent = ({
  tab,
  title,
  currentLanguage,
  notificationSubMenu,
  tenant,
  isLoading,
  error,
  children,
}) => {
  useEffect(() => {
    if (title) {
      document.title = title;
    }
  }, [title]);

  return (
    <>
      <Route
        path={['/notifications', '/settings']}
        render={() => (
          <div className="row">
            <button
              id="toggle-sidebar"
              type="button"
              className="navbar-toggle btn btn-sm btn-access-negative float-left mr-2"
              data-toggle="collapse"
              data-target="#sidebar"
              aria-expanded="false"
              aria-controls="sidebar">
              <span className="sr-only">Toggle sidebar</span>
              <span className="chevron" />
            </button>
            <nav className="col-md-2 d-md-block sidebar collapse" id="sidebar">
              <div className="sidebar-sticky">
                <ul className="nav flex-column mt-3">
                  <li className="nav-item">
                    <Link
                      className={`nav-link ${tab === 'Me' ? 'active' : ''}`}
                      to={'/settings/me'}>
                      <i className="fas fa-user" />
                      <Translation i18nkey="My profile" language={currentLanguage}>
                        My profile
                      </Translation>
                    </Link>
                  </li>
                  <li className="nav-item">
                    <Link
                      className={`nav-link ${tab === 'Notifications' ? 'active' : ''}`}
                      to={'/notifications'}>
                      <i className="fas fa-bell" />
                      <Translation i18nkey="Notifications" language={currentLanguage}>
                        Notifications
                      </Translation>
                    </Link>
                    {notificationSubMenu || null}
                  </li>
                </ul>

                <Can I={manage} a={TENANT}>
                  <h6 className="sidebar-heading d-flex justify-content-between align-items-center px-3 mt-4 mb-1 text-muted">
                    <Translation i18nkey="Tenant administration" language={currentLanguage}>
                      Tenant administration
                    </Translation>
                    <Link
                      to={`/settings/tenants/${tenant._humanReadableId}`}
                      className=""
                      title={t('Update tenant', currentLanguage)}>
                      <i className="fas fa-pen" />
                    </Link>
                  </h6>
                  <ul className="nav flex-column mb-2">
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Messages' ? 'active' : ''}`}
                        to={'/settings/messages'}>
                        <i className="fas fa-comment-alt" />
                        <Translation i18nkey="Message" language={currentLanguage} isPlural>
                          Messages
                        </Translation>
                      </Link>
                    </li>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Otoroshi' ? 'active' : ''}`}
                        to={'/settings/otoroshis'}>
                        <i className="fas fa-pastafarianism" />
                        <Translation
                          i18nkey="Otoroshi instance"
                          language={currentLanguage}
                          isPlural>
                          Otoroshi instances
                        </Translation>
                      </Link>
                    </li>
                    <li className="nav-item">
                      <Link
                        className={`mr-1 nav-link ${tab === 'Admins' ? 'active' : ''}`}
                        to={'/settings/admins'}>
                        <i className="fas fa-user-shield mr-1" />
                        <Translation i18nkey="Admins" language={currentLanguage}>
                          Admins
                        </Translation>
                      </Link>
                    </li>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Audit trail' ? 'active' : ''}`}
                        to={'/settings/audit'}>
                        <i className="fas fa-book" />
                        <Translation i18nkey="Audit trail" language={currentLanguage}>
                          Audit trail
                        </Translation>
                      </Link>
                    </li>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Teams' ? 'active' : ''}`}
                        to={'/settings/teams'}>
                        <i className="fas fa-user-friends" />
                        <Translation i18nkey="Teams" language={currentLanguage}>
                          Teams
                        </Translation>
                      </Link>
                    </li>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Assets' ? 'active' : ''}`}
                        to={'/settings/assets'}>
                        <i className="fas fa-tools" />
                        <Translation i18nkey="Tenant assets" language={currentLanguage}>
                          Tenant assets
                        </Translation>
                      </Link>
                    </li>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Initialization' ? 'active' : ''}`}
                        to={'/settings/init'}>
                        <i className="fas fa-cloud-download-alt" />
                        <Translation i18nkey="Initialization" language={currentLanguage}>
                          Initalization
                        </Translation>
                      </Link>
                    </li>
                  </ul>
                </Can>

                <Can I={manage} a={daikoku}>
                  <h6 className="sidebar-heading d-flex justify-content-between align-items-center px-3 mt-4 mb-1 text-muted">
                    <span>
                      <Translation i18nkey="Daikoku administration" language={currentLanguage}>
                        Daikoku administration
                      </Translation>
                    </span>
                  </h6>
                  <ul className="nav flex-column mb-2">
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Tenants' ? 'active' : ''}`}
                        to={'/settings/tenants'}>
                        <i className="fas fa-globe" />

                        <Translation i18nkey="Tenants" language={currentLanguage}>
                          Tenants
                        </Translation>
                      </Link>
                    </li>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Users' ? 'active' : ''}`}
                        to={'/settings/users'}>
                        <i className="fas fa-users" />
                        <Translation i18nkey="Users" language={currentLanguage}>
                          Users
                        </Translation>
                      </Link>
                    </li>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'User sessions' ? 'active' : ''}`}
                        to={'/settings/sessions'}>
                        <i className="fas fa-address-card" />
                        <Translation i18nkey="User sessions" language={currentLanguage}>
                          User sessions
                        </Translation>
                      </Link>
                    </li>
                    <li className="nav-item">
                      <Link
                        className={`nav-link ${tab === 'Import / Export' ? 'active' : ''}`}
                        to={'/settings/import-export'}>
                        <i className="fas fa-download" />
                        <Translation i18nkey="Import / Export" language={currentLanguage}>
                          Import / Export
                        </Translation>
                      </Link>
                    </li>
                  </ul>
                </Can>
              </div>
            </nav>
            <main role="main" className="col-md-10 ml-sm-auto px-4">
              <div className={classNames('back-office-overlay', { active: isLoading })} />
              <BackOfficeContent error={error}>{children}</BackOfficeContent>
            </main>
          </div>
        )}
      />
    </>
  );
};

const mapStateToProps = (state) => ({
  ...state.context,
  error: state.error,
});

export const TeamBackOffice = connect(mapStateToProps)(TeamBackOfficeComponent);
export const UserBackOffice = connect(mapStateToProps)(UserBackOfficeComponent);

export const TeamBackOfficeHome = connect(mapStateToProps)(TeamBackOfficeHomeComponent);
