import { extend } from "lodash";
import React, { useCallback } from "react";
import Modal from "antd/lib/modal";
import { Query } from "@/services/query";
import notification from "@/services/notification";
import useImmutableCallback from "@/lib/hooks/useImmutableCallback";

function confirmArchive() {
  return new Promise((resolve, reject) => {
    Modal.confirm({
      title: window.W_L.archived_search,
      content: (
        <React.Fragment>
          <div className="m-b-5">{window.W_L.archived_confirm}</div>
          <div>{window.W_L.archived_tip}</div>
        </React.Fragment>
      ),
      okText: window.W_L.ok_text,
      cancelText: window.W_L.cancel,
      okType: "danger",
      onOk: () => {
        resolve();
      },
      onCancel: () => {
        reject();
      },
      maskClosable: true,
      autoFocusButton: null,
    });
  });
}

function doArchiveQuery(query) {
  return Query.delete({ id: query.id })
    .then(() => {
      return extend(query.clone(), { is_archived: true, schedule: null });
    })
    .catch(error => {
      notification.error(window.W_L.archived_failed);
      return Promise.reject(error);
    });
}

export default function useArchiveQuery(query, onChange) {
  const handleChange = useImmutableCallback(onChange);

  return useCallback(() => {
    confirmArchive()
      .then(() => doArchiveQuery(query))
      .then(handleChange);
  }, [query, handleChange]);
}
