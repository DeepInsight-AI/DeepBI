import { isEmpty } from "lodash";
import React, { useEffect, useState } from "react";
import PropTypes from "prop-types";

import Link from "@/components/Link";
import LoadingOutlinedIcon from "@ant-design/icons/LoadingOutlined";

import { Dashboard } from "@/services/dashboard";
import { Query } from "@/services/query";

export function FavoriteList({ title, resource, itemUrl, emptyState }) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    resource
      .favorites()
      .then(({ results }) => setItems(results))
      .finally(() => setLoading(false));
  }, [resource]);

  return (
    <>
      <div className="d-flex align-items-center m-b-20">
        <p className="flex-fill f-500 c-black m-0">{title}</p>
        {loading && <LoadingOutlinedIcon />}
      </div>
      {!isEmpty(items) && (
        <div role="list" className="list-group">
          {items.map(item => (
            <Link key={itemUrl(item)} role="listitem" className="list-group-item" href={itemUrl(item)}>
              <span className="btn-favorite m-r-5">
                <i className="fa fa-star" aria-hidden="true" />
              </span>
              {item.name}
              {item.is_draft && <span className="label label-default m-l-5">{window.W_L.unpublished}</span>}
            </Link>
          ))}
        </div>
      )}
      {isEmpty(items) && !loading && emptyState}
    </>
  );
}

FavoriteList.propTypes = {
  title: PropTypes.string.isRequired,
  resource: PropTypes.func.isRequired, // eslint-disable-line react/forbid-prop-types
  itemUrl: PropTypes.func.isRequired,
  emptyState: PropTypes.node,
};
FavoriteList.defaultProps = { emptyState: null };

export function DashboardAndQueryFavoritesList() {
  return (
    <div className="tile">
      <div className="t-body tb-padding">
        <div className="row home-favorites-list">
          <div className="col-sm-6 m-t-20">
            <FavoriteList
              title={window.W_L.my_favorite_dashboard}
              resource={Dashboard}
              itemUrl={dashboard => dashboard.url}
              emptyState={
                <p>
                  <span className="btn-favorite m-r-5">
                    <i className="fa fa-star" aria-hidden="true" />
                  </span>
                  <Link href="dashboards">{window.W_L.my_favorite_dashboard}</Link>
                </p>
              }
            />
          </div>
          <div className="col-sm-6 m-t-20">
            <FavoriteList
              title={window.W_L.my_favorite_query}
              resource={Query}
              itemUrl={query => `queries/${query.id}`}
              emptyState={
                <p>
                  <span className="btn-favorite m-r-5">
                    <i className="fa fa-star" aria-hidden="true" />
                  </span>
                  <Link href="queries">{window.W_L.my_favorite_query}</Link>
                </p>
              }
            />
          </div>
        </div>
      </div>
    </div>
  );
}
