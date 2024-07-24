import React, { useState, useEffect, useCallback } from "react";
import { axios } from "@/services/axios";
import PropTypes from "prop-types";
import { each, debounce, get, find } from "lodash";
import Button from "antd/lib/button";
import List from "antd/lib/list";
import Modal from "antd/lib/modal";
import Select from "antd/lib/select";
import Tag from "antd/lib/tag";
import Tooltip from "@/components/Tooltip";
import { wrap as wrapDialog, DialogPropType } from "@/components/DialogWrapper";
import { toHuman } from "@/lib/utils";
import HelpTrigger from "@/components/HelpTrigger";
import { UserPreviewCard } from "@/components/PreviewCard";
import PlainButton from "@/components/PlainButton";
import notification from "@/services/notification";
import User from "@/services/user";

import "./index.less";

const { Option } = Select;
const DEBOUNCE_SEARCH_DURATION = 200;

function useGrantees(url) {
  const loadGrantees = useCallback(
    () =>
      axios.get(url).then(data => {
        const resultGrantees = [];
        each(data, (grantees, accessType) => {
          grantees.forEach(grantee => {
            grantee.accessType = toHuman(accessType);
            resultGrantees.push(grantee);
          });
        });
        return resultGrantees;
      }),
    [url]
  );

  const addPermission = useCallback(
    (userId, accessType = "modify") =>
      axios
        .post(url, { access_type: accessType, user_id: userId })
        .catch(() => notification.error(window.W_L.unauthorized)),
    [url]
  );

  const removePermission = useCallback(
    (userId, accessType = "modify") =>
      axios
        .delete(url, { data: { access_type: accessType, user_id: userId } })
        .catch(() => notification.error(window.W_L.can_not_delete_user)),
    [url]
  );

  return { loadGrantees, addPermission, removePermission };
}

const searchUsers = searchTerm =>
  User.query({ q: searchTerm })
    .then(({ results }) => results)
    .catch(() => []);

function PermissionsEditorDialogHeader({ context }) {
  return (
    <>
      {window.W_L.mange_permissions}
      <div className="modal-header-desc">
        {window.W_L.can_not_manage_permissions}
        <HelpTrigger type="MANAGE_PERMISSIONS" />
      </div>
    </>
  );
}

PermissionsEditorDialogHeader.propTypes = { context: PropTypes.oneOf([window.W_L.queries, window.W_L.dashboards]) };
PermissionsEditorDialogHeader.defaultProps = { context: window.W_L.queries };

function UserSelect({ onSelect, shouldShowUser }) {
  const [loadingUsers, setLoadingUsers] = useState(true);
  const [users, setUsers] = useState([]);
  const [searchTerm, setSearchTerm] = useState("");

  const debouncedSearchUsers = useCallback(
    debounce(
      search =>
        searchUsers(search)
          .then(setUsers)
          .finally(() => setLoadingUsers(false)),
      DEBOUNCE_SEARCH_DURATION
    ),
    []
  );

  useEffect(() => {
    setLoadingUsers(true);
    debouncedSearchUsers(searchTerm);
  }, [debouncedSearchUsers, searchTerm]);

  return (
    <Select
      className="w-100 m-b-10"
      placeholder={window.W_L.add_user}
      showSearch
      onSearch={setSearchTerm}
      suffixIcon={
        loadingUsers ? (
          <span role="status" aria-live="polite" aria-relevant="additions removals">
            <i className="fa fa-spinner fa-pulse" aria-hidden="true" />
            <span className="sr-only">Loading...</span>
          </span>
        ) : (
          <i className="fa fa-search" aria-hidden="true" />
        )
      }
      filterOption={false}
      notFoundContent={null}
      value={undefined}
      getPopupContainer={trigger => trigger.parentNode}
      onSelect={onSelect}>
      {users.filter(shouldShowUser).map(user => (
        <Option key={user.id} value={user.id}>
          <UserPreviewCard user={user} />
        </Option>
      ))}
    </Select>
  );
}

UserSelect.propTypes = {
  onSelect: PropTypes.func,
  shouldShowUser: PropTypes.func,
};
UserSelect.defaultProps = { onSelect: () => {}, shouldShowUser: () => true };

function PermissionsEditorDialog({ dialog, author, context, aclUrl }) {
  const [loadingGrantees, setLoadingGrantees] = useState(true);
  const [grantees, setGrantees] = useState([]);
  const { loadGrantees, addPermission, removePermission } = useGrantees(aclUrl);
  const loadUsersWithPermissions = useCallback(() => {
    setLoadingGrantees(true);
    loadGrantees()
      .then(setGrantees)
      .catch(() => notification.error(window.W_L.fail_load_list))
      .finally(() => setLoadingGrantees(false));
  }, [loadGrantees]);

  const userHasPermission = useCallback(
    user => user.id === author.id || !!get(find(grantees, { id: user.id }), "accessType"),
    [author.id, grantees]
  );

  useEffect(() => {
    loadUsersWithPermissions();
  }, [aclUrl, loadUsersWithPermissions]);

  return (
    <Modal
      {...dialog.props}
      className="permissions-editor-dialog"
      title={<PermissionsEditorDialogHeader context={context} />}
      footer={<Button onClick={dialog.dismiss}>{window.W_L.close}</Button>}>
      <UserSelect
        onSelect={userId => addPermission(userId).then(loadUsersWithPermissions)}
        shouldShowUser={user => !userHasPermission(user)}
      />
      <div className="d-flex align-items-center m-t-5">
        <h5 className="flex-fill">{window.W_L.user_permissions}</h5>
        {loadingGrantees && (
          <span role="status" aria-live="polite" aria-relevant="additions removals">
            <i className="fa fa-spinner fa-pulse" aria-hidden="true" />
            <span className="sr-only">{window.W_L.loading}</span>
          </span>
        )}
      </div>
      <div className="scrollbox p-5" style={{ maxHeight: "40vh" }}>
        <List
          size="small"
          dataSource={[author, ...grantees]}
          renderItem={user => (
            <List.Item>
              <UserPreviewCard key={user.id} user={user}>
                {user.id === author.id ? (
                  <Tag className="m-0">{window.W_L.author}</Tag>
                ) : (
                  <Tooltip title="{window.W_L.remove_user_permission}">
                    <PlainButton
                      aria-label="{window.W_L.remove_user_permission}"
                      onClick={() => removePermission(user.id).then(loadUsersWithPermissions)}>
                      <i className="fa fa-remove clickable" aria-hidden="true" />
                    </PlainButton>
                  </Tooltip>
                )}
              </UserPreviewCard>
            </List.Item>
          )}
        />
      </div>
    </Modal>
  );
}

PermissionsEditorDialog.propTypes = {
  dialog: DialogPropType.isRequired,
  author: PropTypes.object.isRequired, // eslint-disable-line react/forbid-prop-types
  context: PropTypes.oneOf([window.W_L.query, window.W_L.dashboards]),
  aclUrl: PropTypes.string.isRequired,
};

PermissionsEditorDialog.defaultProps = { context: window.W_L.query };

export default wrapDialog(PermissionsEditorDialog);
