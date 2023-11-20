import { get, map } from "lodash";
import React, { useMemo, useCallback } from "react";
import PropTypes from "prop-types";
import { UserProfile } from "@/components/proptypes";
import DynamicComponent from "@/components/DynamicComponent";
import DynamicForm from "@/components/dynamic-form/DynamicForm";
import UserGroups from "@/components/UserGroups";

import User from "@/services/user";
import { currentUser } from "@/services/auth";
import useImmutableCallback from "@/lib/hooks/useImmutableCallback";

import useUserGroups from "../hooks/useUserGroups";

export default function UserInfoForm(props) {
  const { user, onChange } = props;

  const { groups, allGroups, isLoading: isLoadingGroups } = useUserGroups(user);

  const handleChange = useImmutableCallback(onChange);

  const saveUser = useCallback(
    (values, successCallback, errorCallback) => {
      const data = {
        ...values,
        id: user.id,
      };

      User.save(data)
        .then(user => {
          successCallback(window.W_L.save_success);
          handleChange(User.convertUserInfo(user));
        })
        .catch(error => {
          errorCallback(get(error, "response.data.message", "Failed saving."));
        });
    },
    [user, handleChange]
  );

  const formFields = useMemo(
    () =>
      map(
        [
          {
            name: "name",
            title: window.W_L.name,
            type: "text",
            initialValue: user.name,
          },
          {
            name: "email",
            title: window.W_L.email,
            type: "email",
            initialValue: user.email,
          },
          !user.isDisabled && currentUser.id !== user.id
            ? {
                name: "group_ids",
                title: window.W_L.role,
                type: "select",
                mode: "multiple",
                options: map(allGroups, group => ({ name: group.name, value: group.id })),
                initialValue: user.groupIds,
                loading: isLoadingGroups,
                placeholder: isLoadingGroups ? window.W_L.loading : "",
              }
            : {
                name: "group_ids",
                title: window.W_L.role,
                type: "content",
                required: false,
                content: isLoadingGroups ? window.W_L.loading : <UserGroups data-test="Groups" groups={groups} />,
              },
        ],
        field => ({ readOnly: user.isDisabled, required: true, ...field })
      ),
    [user, groups, allGroups, isLoadingGroups]
  );

  return (
    <DynamicComponent name="UserProfile.UserInfoForm" {...props}>
      <DynamicForm fields={formFields} onSubmit={saveUser} hideSubmitButton={user.isDisabled} />
    </DynamicComponent>
  );
}

UserInfoForm.propTypes = {
  user: UserProfile.isRequired,
  onChange: PropTypes.func,
};

UserInfoForm.defaultProps = {
  onChange: () => {},
};
