import { isString, get, find } from "lodash";
import sanitize from "@/services/sanitize";
import { axios } from "@/services/axios";
import notification from "@/services/notification";
import { clientConfig } from "@/services/auth";

function getErrorMessage(error) {
  return find([get(error, "response.data.message"), get(error, "response.statusText"), "Unknown error"], isString);
}

function disableResource(user) {
  return `api/users/${user.id}/disable`;
}

function enableUser(user) {
  const userName = sanitize(user.name);

  return axios
    .delete(disableResource(user))
    .then(data => {
      notification.success(window.W_L.account +userName + window.W_L.activated + '！');
      user.is_disabled = false;
      user.profile_image_url = data.profile_image_url;
      return data;
    }) 
    .catch(error => {
      notification.error(window.W_L.Cannot_enable_user, getErrorMessage(error));
    });
}

function disableUser(user) {
  const userName = sanitize(user.name);
  return axios
    .post(disableResource(user))
    .then(data => {
      notification.warning(window.W_L.account +userName + window.W_L.terminated + '。');
      user.is_disabled = true;
      user.profile_image_url = data.profile_image_url;
      return data;
    })
    .catch(error => {
      notification.error(window.W_L.Cannot_deactivate_user, getErrorMessage(error));
    });
}

function deleteUser(user) {
  const userName = sanitize(user.name);
  return axios
    .delete(`api/users/${user.id}`)
    .then(data => {
      notification.warning(window.W_L.account +userName + window.W_L.has_been_deleted + '!');
      return data;
    })
    .catch(error => {
      notification.error(window.W_L.Cannot_delete_user, getErrorMessage(error));
    });
}

function convertUserInfo(user) {
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    profileImageUrl: user.profile_image_url,
    apiKey: user.api_key,
    groupIds: user.groups,
    isDisabled: user.is_disabled,
    isInvitationPending: user.is_invitation_pending,
  };
}

function regenerateApiKey(user) {
  return axios
    .post(`api/users/${user.id}/regenerate_api_key`)
    .then(data => {
      notification.success(window.W_L.api_Key_has_been_regenerated);
      return data.api_key;
    })
    .catch(error => {
      notification.error(window.W_L.failed_to_regenerate_api_key, getErrorMessage(error));
    });
}

function sendPasswordReset(user) {
  return axios
    .post(`api/users/${user.id}/reset_password`)
    .then(data => {
      if (clientConfig.mailSettingsMissing) {
        notification.warning(window.W_L.the_email_server_is_not_configured);
        return data.reset_link;
      }
      notification.success(window.W_L.password_reset_email_has_been_sent);
    })
    .catch(error => {
      notification.error(window.W_L.the_password_reset_email_was_not_sent_successfully, getErrorMessage(error));
    });
}

function resendInvitation(user) {
  return axios
    .post(`api/users/${user.id}/invite`)
    .then(data => {
      if (clientConfig.mailSettingsMissing) {
        notification.warning(window.W_L.the_email_server_is_not_configured);
        return data.invite_link;
      }
      notification.success(window.W_L.user_invitation_email_has_been_sent);
    })
    .catch(error => {
      notification.error(window.W_L.the_user_invitation_email_failed_to_be_sent, getErrorMessage(error));
    });
}

const User = {
  query: params => axios.get("api/users", { params }),
  get: ({ id }) => axios.get(`api/users/${id}`),
  create: data => axios.post(`api/users`, data),
  save: data => axios.post(`api/users/${data.id}`, data),
  enableUser,
  disableUser,
  deleteUser,
  convertUserInfo,
  regenerateApiKey,
  sendPasswordReset,
  resendInvitation,
};

export default User;
