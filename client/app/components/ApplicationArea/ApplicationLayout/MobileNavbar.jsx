import { first } from "lodash";
import React from "react";
import PropTypes from "prop-types";
import Button from "antd/lib/button";
import MenuOutlinedIcon from "@ant-design/icons/MenuOutlined";
import Dropdown from "antd/lib/dropdown";
import Menu from "antd/lib/menu";
import Link from "@/components/Link";
import { Auth, currentUser } from "@/services/auth";
import settingsMenu from "@/services/settingsMenu";
import logoUrl from "@/assets/images/icon_small.png";

import "./MobileNavbar.less";

export default function MobileNavbar({ getPopupContainer }) {
  const firstSettingsTab = first(settingsMenu.getAvailableItems());

  return (
    <div className="mobile-navbar">
      <div className="mobile-navbar-logo">
        <Link href="./">
          <img src={logoUrl} alt="DeepBI" />
        </Link>
      </div>
      <div>
        <Dropdown
          overlayStyle={{ minWidth: 200 }}
          trigger={["click"]}
          getPopupContainer={getPopupContainer} // so the overlay menu stays with the fixed header when page scrolls
          overlay={
            <Menu mode="vertical" theme="dark" selectable={false} className="mobile-navbar-menu">
              {/* {currentUser.hasPermission("list_dashboards") && ( */}
                <Menu.Item key="testdialogue">
                  <Link href="./">{window.W_L.data_analysis}</Link>
                </Menu.Item>
                <Menu.Item key="dialogue-list">
                  <Link href="dialogue-list">{window.W_L.history_dialogue}</Link>
                </Menu.Item>
               {/* )} */}
              {/* {currentUser.hasPermission("view_query") && ( */}
                <Menu.Item key="report-route">
                  <Link href="report-route">{window.W_L.report_generation}</Link>
                </Menu.Item>
              {/* )} */}
              {/* {currentUser.hasPermission("list_alerts") && ( */}
                <Menu.Item key="queries">
                  <Link href="queries">{window.W_L.report_list}</Link>
                </Menu.Item>
              {/* )} */}
              <Menu.Item key="dashboards">
                <Link href="dashboards">{window.W_L.dashboard}</Link>
              </Menu.Item>
              <Menu.Item key="dashboards_prettify">
                  <Link href="dashboards_prettify">{window.W_L.dashboards_prettify}</Link>
                </Menu.Item>
              <Menu.Item key="autopilot">
                <Link href="autopilot">{window.W_L.auto_pilot}</Link>
              </Menu.Item>
              <Menu.Item key="autopilot_list"> 
                <Link href="autopilot_list">{window.W_L.history_autopilot}</Link>
              </Menu.Item> 
              {/* <Menu.Divider /> */}
              {firstSettingsTab && (
                <Menu.Item key="settings">
                  <Link href={firstSettingsTab.path}>{window.W_L.setting}</Link>
                </Menu.Item>
              )}
              {/* {currentUser.hasPermission("super_admin") && (
                <Menu.Item key="status">
                  <Link href="admin/status">{window.W_L.sys_status}</Link>
                </Menu.Item>
              )} */}
              {currentUser.hasPermission("super_admin") && <Menu.Divider />}
              {/* <Menu.Item key="help">
                <Link href="" target="_blank" rel="noopener">
                  {window.W_L.help}
                </Link>
              </Menu.Item> */}
              
              <Menu.Item key="logout" onClick={() => Auth.logout()}>
                {window.W_L.log_out}
              </Menu.Item>
            </Menu>
          }>
          <Button className="mobile-navbar-toggle-button" ghost>
            <MenuOutlinedIcon />
          </Button>
        </Dropdown>
      </div>
    </div>
  );
}

MobileNavbar.propTypes = {
  getPopupContainer: PropTypes.func,
};

MobileNavbar.defaultProps = {
  getPopupContainer: null,
};
