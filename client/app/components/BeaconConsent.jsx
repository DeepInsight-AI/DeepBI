import React, { useState } from "react";
import Card from "antd/lib/card";
import Button from "antd/lib/button";
import Typography from "antd/lib/typography";
import { clientConfig } from "@/services/auth";
import Link from "@/components/Link";
import HelpTrigger from "@/components/HelpTrigger";
import DynamicComponent from "@/components/DynamicComponent";
import OrgSettings from "@/services/organizationSettings";

const Text = Typography.Text;

function BeaconConsent() {
  const [hide, setHide] = useState(false);

  if (!clientConfig.showBeaconConsentMessage || hide) {
    return null;
  }

  const hideConsentCard = () => {
    clientConfig.showBeaconConsentMessage = false;
    setHide(true);
  };

  const confirmConsent = confirm => {
    let message = window.W_L.thank_you;

    if (!confirm) {
      message = window.W_L.setting_saved_success;
    }

    OrgSettings.save({ beacon_consent: confirm }, message)
      // .then(() => {
      //   // const settings = get(response, 'settings');
      //   // this.setState({ settings, formValues: { ...settings } });
      // })
      .finally(hideConsentCard);
  };

  return (
    <DynamicComponent name="BeaconConsent">
      <div className="m-t-10 tiled">
        <Card
          title={
            <>
              {window.W_L.share_info}{" "}
              <HelpTrigger type="USAGE_DATA_SHARING" />
            </>
          }
          bordered={false}>
          <Text>{window.W_L.help_redash}：</Text>
          <div className="m-t-5">
            <ul>
              <li> {window.W_L.redash_about_1}</li>
              <li> {window.W_L.redash_about_2}</li>
            </ul>
          </div>
          <Text>{window.W_L.redash_info}</Text>
          <div className="m-t-5">
            <Button type="primary" className="m-r-5" onClick={() => confirmConsent(true)}>
              {window.W_L.accept}
            </Button>
            <Button type="default" onClick={() => confirmConsent(false)}>
              {window.W_L.reject}
            </Button>
          </div>
          <div className="m-t-15">
            <Text type="secondary">
              {window.W_L.set_info_in}{" "}
              <Link href="settings/general">{window.W_L.sys_setting}</Link> {window.W_L.sys_setting_add}。
            </Text>
          </div>
        </Card>
      </div>
    </DynamicComponent>
  );
}

export default BeaconConsent;
