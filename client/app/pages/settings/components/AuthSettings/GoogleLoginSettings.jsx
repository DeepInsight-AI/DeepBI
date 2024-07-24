import { isEmpty, join } from "lodash";
import React from "react";
import Form from "antd/lib/form";
import Select from "antd/lib/select";
import Alert from "antd/lib/alert";
import DynamicComponent from "@/components/DynamicComponent";
import { clientConfig } from "@/services/auth";
import { SettingsEditorPropTypes, SettingsEditorDefaultProps } from "../prop-types";

export default function GoogleLoginSettings(props) {
  const { values, onChange } = props;

  if (!clientConfig.googleLoginEnabled) {
    return null;
  }

  return (
    <DynamicComponent name="OrganizationSettings.GoogleLoginSettings" {...props}>
      <h4>{window.W_L.google_login}</h4>
      <Form.Item label="{window.W_L.allow} Google Apps Domains">
        <Select
          mode="tags"
          value={values.auth_google_apps_domains}
          onChange={value => onChange({ auth_google_apps_domains: value })}
        />
        {!isEmpty(values.auth_google_apps_domains) && (
          <Alert
            message={
              <p>
                {window.W_L.google_login_tip}<strong>{join(values.auth_google_apps_domains, ", ")}</strong>{window.W_L.google_login_auto_reg}
              </p>
            }
            className="m-t-15"
          />
        )}
      </Form.Item>
    </DynamicComponent>
  );
}

GoogleLoginSettings.propTypes = SettingsEditorPropTypes;

GoogleLoginSettings.defaultProps = SettingsEditorDefaultProps;
