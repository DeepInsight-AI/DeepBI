import React, { useMemo,useEffect,useState } from "react";
import { first, includes } from "lodash";
import Menu from "antd/lib/menu";
import Link from "@/components/Link";
import PlainButton from "@/components/PlainButton";
import HelpTrigger from "@/components/HelpTrigger";
import { useCurrentRoute } from "@/components/ApplicationArea/Router";
import { Auth, currentUser } from "@/services/auth";
import settingsMenu from "@/services/settingsMenu";
import logoUrl from "@/assets/images/icon_small.png";


import CommentOutlinedIcon from "@ant-design/icons/CommentOutlined";
import LineChartOutlinedIcon from "@ant-design/icons/LineChartOutlined";
import DashboardOutlinedIcon from "@ant-design/icons/DashboardOutlined";
import RobotOutlinedIcon from "@ant-design/icons/RobotOutlined";

import QuestionCircleOutlinedIcon from "@ant-design/icons/QuestionCircleOutlined";
import SettingOutlinedIcon from "@ant-design/icons/SettingOutlined";
import VersionInfo from "./VersionInfo";

import {lockReconnectEvent} from "@/pages/testdialogue/components/Dialogue/websocket.js"

import "./DesktopNavbar.less";

function NavbarSection({ children, ...props }) {
  return (
    <Menu selectable={false} mode="vertical" theme="dark" {...props}>
      {children}
    </Menu>
  );
}

function useNavbarActiveState() {
  const currentRoute = useCurrentRoute();

  return useMemo(
    () => ({
      dashboards: includes(
        [
          "Dashboards.List",
          "Dashboards.Favorites",
          "Dashboards.My",
          "Dashboards.ViewOrEdit",
          "Dashboards.LegacyViewOrEdit",
        ],
        currentRoute.id
      ),
      queries: includes(
        [
          "Queries.List",
          "Queries.Favorites",
          "Queries.Archived",
          "Queries.My",
          "Queries.View",
          "Queries.New",
          "Queries.Edit",
        ],
        currentRoute.id
      ),
      dataSources: includes(["DataSources.List"], currentRoute.id),
      alerts: includes(["Alerts.List", "Alerts.New", "Alerts.View", "Alerts.Edit"], currentRoute.id),
      testdialogue: includes(["Dialogue.List"], currentRoute.id),
      report_route : includes(["Dialogue.List.Report"], currentRoute.id),
      dialogue_list : includes(["Dialogue.List.Dialogue"], currentRoute.id),
      autopilot : includes(["Dialogue.List.autopilot"], currentRoute.id),
      autopilot_list : includes(["Dialogue.List.autopilot_list"], currentRoute.id),
      autopilot_view : includes(["Dialogue.List.autopilot_view"], currentRoute.id),
      dashboards_prettify : includes(["Dialogue.List.dashboards_prettify"], currentRoute.id),
    }),
    [currentRoute.id]
  );
}

export default function DesktopNavbar() {
  const firstSettingsTab = first(settingsMenu.getAvailableItems());

  const activeState = useNavbarActiveState();

  // const canCreateQuery = currentUser.hasPermission("create_query");
  // const canCreateDashboard = currentUser.hasPermission("create_dashboard");
  // const canCreateAlert = currentUser.hasPermission("list_alerts");

  const [socketType,setSocketType] = useState(0)
  useEffect(() => {
      const handleLockReconnectChange = (newLockReconnect) => {
        setSocketType(newLockReconnect)
      };
    
      lockReconnectEvent.on('change', handleLockReconnectChange);
    
      return () => {
        lockReconnectEvent.off('change', handleLockReconnectChange);
      };
  }, []);
  return (
    <nav className="desktop-navbar">
      <NavbarSection className="desktop-navbar-logo">
        <div role="menuitem">
          <Link href="./">
            <img src={logoUrl} alt="DeepBI" />
          </Link>
        </div>
      </NavbarSection>
      <NavbarSection className="desktop-navbar-spacer" style={{flex:"0"}}>
          <Menu.SubMenu
            key="create"
            popupClassName="desktop-navbar-submenu"
            data-test="dialogueButton"
            className={activeState.testdialogue || activeState.dialogue_list ? "navbar-active-item" : null}
            tabIndex={0}
            title={
              <React.Fragment>
                <CommentOutlinedIcon />
                <span className="desktop-navbar-label">{window.W_L.data_analysis}</span>
              </React.Fragment>
            }>
              <Menu.Item key="testdialogue">
                <Link href="./" data-test="testdialogue">
                  {window.W_L.dialogue}
                </Link>
              </Menu.Item>
              <Menu.Item key="dialogue_list">
                <Link data-test="dialogue-list" href="dialogue-list">
                  {window.W_L.history_dialogue}
                </Link>
              </Menu.Item>
          </Menu.SubMenu>
      </NavbarSection>
      <NavbarSection className="desktop-navbar-spacer" style={{flex:"0"}}>
          <Menu.SubMenu
            key="create"
            popupClassName="desktop-navbar-submenu"
            data-test="queriesButton"
            tabIndex={0}
            className={activeState.report_route || activeState.queries ? "navbar-active-item" : null}
            title={
              <React.Fragment>
                <LineChartOutlinedIcon />
                <span className="desktop-navbar-label">{window.W_L.query_builder}</span>
              </React.Fragment>
            }>
              <Menu.Item key="report_route">
                <Link href="report-route" data-test="report-route">
                  {window.W_L.report_generation}
                </Link>
              </Menu.Item>
              <Menu.Item key="queries">
                <Link data-test="queries" href="queries">
                  {window.W_L.report_list}
                </Link>
              </Menu.Item>
          </Menu.SubMenu>
      </NavbarSection>

        

      <NavbarSection style={{flex:"0"}}>

      <Menu.SubMenu
            key="create"
            popupClassName="desktop-navbar-submenu"
            data-test="queriesButton"
            tabIndex={0}
            className={activeState.dashboards || activeState.dashboards_prettify ? "navbar-active-item" : null}
            title={
              <React.Fragment>
                <DashboardOutlinedIcon />
                <span className="desktop-navbar-label">{window.W_L.dashboards}</span>
              </React.Fragment>
            }>
              <Menu.Item key="dashboards">
                <Link href="dashboards" data-test="dashboards">
                  {window.W_L.dashboards}
                </Link>
              </Menu.Item>

              <Menu.Item key="dashboards_prettify">
                <Link href="dashboards_prettify" data-test="dashboards_prettify">
                  {window.W_L.dashboards_prettify}
                </Link>
              </Menu.Item>
      </Menu.SubMenu>

          {/* <Menu.Item key="autopilot" className={activeState.autopilot ? "navbar-active-item" : null}>
            <Link href="autopilot">
              <RobotOutlinedIcon aria-label="autopilot navigation button" />
              <span className="desktop-navbar-label">{window.W_L.auto_pilot}</span>
            </Link>
          </Menu.Item> */}

      </NavbarSection>
      <NavbarSection className="desktop-navbar-spacer" style={{flex:"1"}}>
          <Menu.SubMenu
            key="create"
            popupClassName="desktop-navbar-submenu"
            data-test="autopilotButton"
            className={activeState.autopilot || activeState.autopilot_view || activeState.autopilot_list ? "navbar-active-item" : null}
            tabIndex={0}
            title={
              <React.Fragment>
                 <RobotOutlinedIcon aria-label="autopilot navigation button" />
                <span className="desktop-navbar-label">{window.W_L.auto_pilot}</span>
              </React.Fragment>
            }>
              <Menu.Item key="autopilot">
                <Link href="autopilot" data-test="autopilot">
                  {window.W_L.auto_pilot}
                </Link>
              </Menu.Item>
              <Menu.Item key="autopilot_list">
                <Link data-test="autopilot_list" href="autopilot_list">
                  {window.W_L.history_autopilot}
                </Link>
              </Menu.Item>
          </Menu.SubMenu>
      </NavbarSection>
 

      <NavbarSection>
        <Menu.Item key="help">
          <HelpTrigger showTooltip={false} type="HOME" tabIndex={0}>
            <QuestionCircleOutlinedIcon />
            <span className="desktop-navbar-label">{window.W_L.help}</span>
          </HelpTrigger>
        </Menu.Item>
        {firstSettingsTab && (
          <Menu.Item key="settings" className={activeState.dataSources ? "navbar-active-item" : null}>
            <Link href={firstSettingsTab.path} data-test="SettingsLink">
              <SettingOutlinedIcon />
              <span className="desktop-navbar-label">{window.W_L.setting}</span>
            </Link>
          </Menu.Item>
        )}
      </NavbarSection>

      <NavbarSection className="desktop-navbar-profile-menu">
        <Menu.SubMenu
          key="profile"
          popupClassName="desktop-navbar-submenu"
          tabIndex={0}
          title={
            <span data-test="ProfileDropdown" className="desktop-navbar-profile-menu-title">
              <img className="profile__image_thumb" src={currentUser.profile_image_url} alt={currentUser.name} />
              <div className="profile__live__type">
                <span style={{"background":socketType===0?"red":socketType===1?"green":"yellow"}}></span>
                <div>{socketType===0?window.W_L.offline:socketType===1?window.W_L.online:window.W_L.pending}</div>
              </div>
            </span>
          }>
          <Menu.Item key="profile">
            <Link href="users/me">{window.W_L.personal_setting}</Link>
          </Menu.Item>
          {currentUser.hasPermission("super_admin") && (
            <Menu.Item key="status">
              <Link href="admin/status">{window.W_L.sys_status}</Link>
            </Menu.Item>
          )}
          <Menu.Divider />
          <Menu.Item key="logout">
            <PlainButton data-test="LogOutButton" onClick={() => Auth.logout()}>
              {window.W_L.log_out}
            </PlainButton>
          </Menu.Item>
          <Menu.Divider />
          <Menu.Item key="version" role="presentation" disabled className="version-info">
            <VersionInfo />
          </Menu.Item>
        </Menu.SubMenu>
      </NavbarSection>
    </nav>
  );
}