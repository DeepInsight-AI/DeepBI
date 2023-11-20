import React from "react";
import Checkbox from "antd/lib/checkbox";
import Form from "antd/lib/form";
import Row from "antd/lib/row";
import Skeleton from "antd/lib/skeleton";
import DynamicComponent from "@/components/DynamicComponent";
import { SettingsEditorPropTypes, SettingsEditorDefaultProps } from "../prop-types";

export default function FeatureFlagsSettings(props) {
  const { values, onChange, loading } = props;

  return (
    <DynamicComponent name="OrganizationSettings.FeatureFlagsSettings" {...props}>
      <Form.Item label={window.W_L.feature_flags}>
        {loading ? (
          <>
            <Row>
              <Skeleton title={false} paragraph={{ width: [300, 300, 300], rows: 3 }} active />
            </Row>
          </>
        ) : (
          <>
            <DynamicComponent name="OrganizationSettings.FeatureFlagsSettings.PermissionsControl" {...props}>
              <Row>
                <Checkbox
                  name="feature_show_permissions_control"
                  checked={values.feature_show_permissions_control}
                  onChange={e => onChange({ feature_show_permissions_control: e.target.checked })}>
					{window.W_L.enable_multiple_owners_tip}
                </Checkbox>
              </Row>
            </DynamicComponent>
            <Row>
              <Checkbox
                name="send_email_on_failed_scheduled_queries"
                checked={values.send_email_on_failed_scheduled_queries}
                onChange={e => onChange({ send_email_on_failed_scheduled_queries: e.target.checked })}>
                {window.W_L.when_query_error_info_by_email}
              </Checkbox>
            </Row>
            <Row>
              <Checkbox
                name="multi_byte_search_enabled"
                checked={values.multi_byte_search_enabled}
                onChange={e => onChange({ multi_byte_search_enabled: e.target.checked })}>
                {window.W_L.dashboard_language_tip}
              </Checkbox>
            </Row>
          </>
        )}
      </Form.Item>
    </DynamicComponent>
  );
}

FeatureFlagsSettings.propTypes = SettingsEditorPropTypes;

FeatureFlagsSettings.defaultProps = SettingsEditorDefaultProps;
