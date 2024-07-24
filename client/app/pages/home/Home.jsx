import { includes } from "lodash";
import React, { useEffect } from "react";

import Alert from "antd/lib/alert";
import Link from "@/components/Link";
import routeWithUserSession from "@/components/ApplicationArea/routeWithUserSession";
import EmptyState, { EmptyStateHelpMessage } from "@/components/empty-state/EmptyState";
import DynamicComponent from "@/components/DynamicComponent";
import BeaconConsent from "@/components/BeaconConsent";
import PlainButton from "@/components/PlainButton";

import { axios } from "@/services/axios";
import recordEvent from "@/services/recordEvent";
import { messages } from "@/services/auth";
import notification from "@/services/notification";
import routes from "@/services/routes";

import { DashboardAndQueryFavoritesList } from "./components/FavoritesList";

import "./Home.less";

function DeprecatedEmbedFeatureAlert() {
  return (
    <Alert
      className="m-b-15"
      type="warning"
      message={
          <>
          你已设置参数 <code>ALLOW_PARAMETERS_IN_EMBEDS</code>，但该特征暂时不可用。{" "}
          <Link
            href="https://discuss.redash.io/t/support-for-parameters-in-embedded-visualizations/3337"
            target="_blank"
            rel="noopener noreferrer">
            Read more
          </Link>
          .
        </>
      }
    />
  );
}

function EmailNotVerifiedAlert() {
  const verifyEmail = () => {
    axios.post("verification_email/").then(data => {
      notification.success(data.message);
    });
  };

  return (
    <Alert
      className="m-b-15"
      type="warning"
      message={
        <>
		  {window.W_L.email_send}{" "}
          <PlainButton type="link" onClick={verifyEmail}>
            {window.W_L.re_send_email}
          </PlainButton>
          .
        </>
      }
    />
  );
}

export default function Home() {
  useEffect(() => {
    recordEvent("view", "page", "personal_homepage");
  }, []);

  return (
    <div className="home-page">
      <div className="container">
        {includes(messages, "using-deprecated-embed-feature") && <DeprecatedEmbedFeatureAlert />}
        {includes(messages, "email-not-verified") && <EmailNotVerifiedAlert />}
        <DynamicComponent name="Home.EmptyState">
          <EmptyState
            header={window.W_L.redash_welcome}
            description={window.W_L.redash_desc}
            illustration="dashboard"
            helpMessage={<EmptyStateHelpMessage helpTriggerType="GETTING_STARTED" />}
            showDashboardStep
            showInviteStep
            onboardingMode
          />
        </DynamicComponent>
        <DynamicComponent name="HomeExtra" />
        <DashboardAndQueryFavoritesList />
        <BeaconConsent />

      </div>
    </div>
  );
}

routes.register(
  "Home",
  routeWithUserSession({
    path: "/home",
    title: "DeepBI",
    render: pageProps => <Home {...pageProps} />,
  })
);
