import { startsWith, get, some, mapValues } from "lodash";
import React from "react";
import PropTypes from "prop-types";
import cx from "classnames";
import Tooltip from "@/components/Tooltip";
import Drawer from "antd/lib/drawer";
import Link from "@/components/Link";
import PlainButton from "@/components/PlainButton";
import CloseOutlinedIcon from "@ant-design/icons/CloseOutlined";
import BigMessage from "@/components/BigMessage";
import HtmlContent from "@/components/chart/components/HtmlContent";
import { axios } from "@/services/axios";
import { markdown } from "markdown";
import DynamicComponent, { registerComponent } from "@/components/DynamicComponent";
import usermdcn from "./user_manual/user_help_cn.md";
import usermden from "./user_manual/user_help_en.md";
import "./HelpTrigger.less";

const DOMAIN = "https://xxx";
const HELP_PATH = "/help";
const IFRAME_TIMEOUT = 20000;
const IFRAME_URL_UPDATE_MESSAGE = "iframe_url";

export const TYPES = mapValues(
  {
    HOME: ["", window.W_L.help],
    VALUE_SOURCE_OPTIONS: ["/user-guide/querying/query-parameters#Value-Source-Options", window.W_L.guide_list_option],
    // SHARE_DASHBOARD: ["/user-guide/dashboards/sharing-dashboards", window.W_L.guide_share_embed_dashboard],
    AUTHENTICATION_OPTIONS: ["/user-guide/users/authentication-options", window.W_L.guide_authentication],
    USAGE_DATA_SHARING: ["/open-source/admin-guide/usage-data", window.W_L.help_share_anonymous_data],
    DS_ATHENA: ["/data-sources/amazon-athena-setup", window.W_L.guide_amazon_athena],
    DS_BIGQUERY: ["/data-sources/bigquery-setup", window.W_L.guide_set_bigquery],
    DS_URL: ["/data-sources/querying-urls", window.W_L.guide_set_url_port],
    DS_MONGODB: ["/data-sources/mongodb-setup", window.W_L.guide_set_mongodb],
    DS_GOOGLE_SPREADSHEETS: [
      "/data-sources/querying-a-google-spreadsheet",
      window.W_L.guide_set_google_spread,
    ],
    DS_GOOGLE_ANALYTICS: ["/data-sources/google-analytics-setup", window.W_L.guide_set_google_analytics],
    DS_AXIBASETSD: ["/data-sources/axibase-time-series-database", window.W_L.guide_set_axibase],
    DS_RESULTS: ["/user-guide/querying/query-results-data-source", window.W_L.guide_set_query_result],
    ALERT_SETUP: ["/user-guide/alerts/setting-up-an-alert", window.W_L.guide_set_alert],
    MAIL_CONFIG: ["/open-source/setup/#Mail-Configuration", window.W_L.guide_set_mail],
    ALERT_NOTIF_TEMPLATE_GUIDE: ["/user-guide/alerts/custom-alert-notifications", window.W_L.guide_set_notice],
    FAVORITES: ["/user-guide/querying/favorites-tagging/#Favorites", window.W_L.guide_favorite],
    MANAGE_PERMISSIONS: [
      "/user-guide/querying/writing-queries#Managing-Query-Permissions",
      window.W_L.guide_query_permissions,
    ],
    NUMBER_FORMAT_SPECS: ["/user-guide/visualizations/formatting-numbers", window.W_L.guide_formatting_number],
    GETTING_STARTED: ["/user-guide/getting-started", window.W_L.guide_menu_start],
    DASHBOARDS: ["/user-guide/dashboards", window.W_L.guide_dashboard],
    QUERIES: ["/help/user-guide/querying", window.W_L.guide_query],
    ALERTS: ["/user-guide/alerts", window.W_L.guide_alert],
  },
  ([url, title]) => [DOMAIN + HELP_PATH + url, title]
);

const HelpTriggerPropTypes = {
  type: PropTypes.string,
  href: PropTypes.string,
  title: PropTypes.node,
  className: PropTypes.string,
  showTooltip: PropTypes.bool,
  renderAsLink: PropTypes.bool,
  children: PropTypes.node,
};

const HelpTriggerDefaultProps = {
  type: null,
  href: null,
  title: null,
  className: null,
  showTooltip: true,
  renderAsLink: false,
  children: <i className="fa fa-question-circle" aria-hidden="true" />,
};


export function helpTriggerWithTypes(types, allowedDomains = [], drawerClassName = null) {
  return class HelpTrigger extends React.Component {
    static propTypes = {
      ...HelpTriggerPropTypes,
      type: PropTypes.oneOf(Object.keys(types)),
    };
    static defaultProps = HelpTriggerDefaultProps;

    iframeRef = React.createRef();

    iframeLoadingTimeout = null;

    state = {
      visible: false,
      loading: false,
      error: false,
      currentUrl: null,
      richText: null
    };

    componentDidMount() {
      window.addEventListener("message", this.onPostMessageReceived, false);
    }

    componentWillUnmount() {
      window.removeEventListener("message", this.onPostMessageReceived);
      clearTimeout(this.iframeLoadingTimeout);
    }

    loadIframe = url => {
      clearTimeout(this.iframeLoadingTimeout);
      this.setState({ loading: true, error: false });

      this.iframeRef.current.src = url;
      this.iframeLoadingTimeout = setTimeout(() => {
        this.setState({ error: url, loading: false });
      }, IFRAME_TIMEOUT); // safety
    };

    onIframeLoaded = () => {
      this.setState({ loading: false });
      clearTimeout(this.iframeLoadingTimeout);
    };

    onPostMessageReceived = event => {
      if (!some(allowedDomains, domain => startsWith(event.origin, domain))) {
        return;
      }

      const { type, message: currentUrl } = event.data || {};
      if (type !== IFRAME_URL_UPDATE_MESSAGE) {
        return;
      }

      this.setState({ currentUrl });
    };

    getUrl = () => {
      const helpTriggerType = get(types, this.props.type);
      return helpTriggerType ? helpTriggerType[0] : this.props.href;
    };
    getRichText =async () => {
      let md = markdown.toHTML(window.W_L.language_mode==="CN"?usermdcn:usermden);
      this.setState({ richText: md,loading: false });
    }
      
    openDrawer = e => {
      if(this.props.type === 'HOME') {
        this.setState({ visible: true,loading: true });
        this.getRichText();
        return;
      }
      // keep "open in new tab" behavior
      if (!e.shiftKey && !e.ctrlKey && !e.metaKey) {
        e.preventDefault();
        this.setState({ visible: true });
        // wait for drawer animation to complete so there's no animation jank
        setTimeout(() => this.loadIframe(this.getUrl()), 300);
      }
    };

    closeDrawer = event => {
      if (event) {
        event.preventDefault();
      }
      this.setState({ visible: false });
      this.setState({ visible: false, currentUrl: null, richText: null });
    };

    render() {
      const targetUrl = this.getUrl();
      if (!targetUrl) {
        return null;
      }

      const tooltip = get(types, `${this.props.type}[1]`, this.props.title);
      const className = cx("help-trigger", this.props.className);
      const url = this.state.currentUrl;
      const isAllowedDomain = some(allowedDomains, domain => startsWith(url || targetUrl, domain));
      const shouldRenderAsLink = this.props.renderAsLink || !isAllowedDomain;
      const type = this.props.type;
      return (
        <React.Fragment>
          <Tooltip
            title={
              this.props.showTooltip ? (
                <>
                  {tooltip}
                  {shouldRenderAsLink && (
                    <>
                      {" "}
                      <i className="fa fa-external-link" style={{ marginLeft: 5 }} aria-hidden="true" />
                      <span className="sr-only">(opens in a new tab)</span>
                    </>
                  )}
                </>
              ) : null
            }>
           {
            type === 'HOME' ?
            <Link  className={className} onClick={this.openDrawer}>
              {this.props.children}
            </Link>
            :
             <Link
             href={url || this.getUrl()}
             className={className}
             rel="noopener noreferrer"
             target="_blank"
             onClick={shouldRenderAsLink ? () => {} : this.openDrawer}>
             
           </Link>
           }
          </Tooltip>
          <Drawer
            placement="right"
            closable={false}
            onClose={this.closeDrawer}
            visible={this.state.visible}
            className={cx("help-drawer", drawerClassName)}
            destroyOnClose
            width={1000}>
            <div className="drawer-wrapper">
              <div className="drawer-menu">
                {url && (
                  <Tooltip title="Open page in a new window" placement="left">
                    {/* eslint-disable-next-line react/jsx-no-target-blank */}
                    <Link href={url} target="_blank">
                      <i className="fa fa-external-link" aria-hidden="true" />
                      <span className="sr-only">(opens in a new tab)</span>
                    </Link>
                  </Tooltip>
                )}
                <Tooltip title="Close" placement="bottom">
                  <PlainButton onClick={this.closeDrawer}>
                    <CloseOutlinedIcon />
                  </PlainButton>
                </Tooltip>
              </div>
              {/* rich text */}
              {this.state.richText &&
                 <HtmlContent className="preview markdown">{this.state.richText}</HtmlContent>
              }
              {/* iframe */}
              {/* {!this.state.error && (
                <iframe
                  ref={this.iframeRef}
                  title="Usage Help"
                  src="about:blank"
                  className={cx({ ready: !this.state.loading })}
                  onLoad={this.onIframeLoaded}
                />
              )} */}

              {/* loading indicator */}
              {this.state.loading && (
                <BigMessage icon="fa-spinner fa-2x fa-pulse" message="Loading..." className="help-message" />
              )}

              {/* error message */}
              {this.state.error && (
                <BigMessage icon="fa-exclamation-circle" className="help-message">
                  Something went wrong.
                  <br />
                  {/* eslint-disable-next-line react/jsx-no-target-blank */}
                  <Link href={this.state.error} target="_blank" rel="noopener">
                    {window.W_L.click_here}
                  </Link>{" "}
                  {window.W_L.open_new_window}
                </BigMessage>
              )}
            </div>

            {/* extra content */}
            <DynamicComponent name="HelpDrawerExtraContent" onLeave={this.closeDrawer} openPageUrl={this.loadIframe} />
          </Drawer>
        </React.Fragment>
      );
    }
  };
}

registerComponent("HelpTrigger", helpTriggerWithTypes(TYPES, [DOMAIN]));

export default function HelpTrigger(props) {
  return <DynamicComponent {...props} name="HelpTrigger" />;
}

HelpTrigger.propTypes = HelpTriggerPropTypes;
HelpTrigger.defaultProps = HelpTriggerDefaultProps;
