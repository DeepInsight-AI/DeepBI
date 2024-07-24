import React from "react";
import Alert from "antd/lib/alert";
import Form from "antd/lib/form";
import Checkbox from "antd/lib/checkbox";
import Tooltip from "@/components/Tooltip";
import Skeleton from "antd/lib/skeleton";
import DynamicComponent from "@/components/DynamicComponent";
import { clientConfig } from "@/services/auth";
import { SettingsEditorPropTypes, SettingsEditorDefaultProps } from "../prop-types";

export default function PasswordLoginSettings(props) {
  const { settings, values, onChange, loading } = props;

  const isTheOnlyAuthMethod =
    !clientConfig.googleLoginEnabled && !clientConfig.ldapLoginEnabled && !values.auth_saml_enabled;

  return (
    <DynamicComponent name="OrganizationSettings.PasswordLoginSettings" {...props}>
      {!loading && !settings.auth_password_login_enabled && (
        <Alert
          message={window.W_L.disable_password_tip}
          type="warning"
          className="m-t-15 m-b-15"
        />
      )}
      <Form.Item label={window.W_L.password_login}>
        {loading ? (
          <Skeleton title={{ width: 300 }} paragraph={false} active />
        ) : (
          <Checkbox
            checked={values.auth_password_login_enabled}
            disabled={isTheOnlyAuthMethod}
            onChange={e => onChange({ auth_password_login_enabled: e.target.checked })}>
            <Tooltip
              title={
                isTheOnlyAuthMethod ? window.W_L.enable_password_login_tip : null
              }
              placement="right">
              {window.W_L.enable_password}
            </Tooltip>
          </Checkbox>
        )}
      </Form.Item>
    </DynamicComponent>
  );
}

PasswordLoginSettings.propTypes = SettingsEditorPropTypes;

PasswordLoginSettings.defaultProps = SettingsEditorDefaultProps;
