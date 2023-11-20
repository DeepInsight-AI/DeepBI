import React from "react";
import PropTypes from "prop-types";
import Link from "@/components/Link";
import BigMessage from "@/components/BigMessage";
import NoTaggedObjectsFound from "@/components/NoTaggedObjectsFound";
import EmptyState, { EmptyStateHelpMessage } from "@/components/empty-state/EmptyState";
import DynamicComponent from "@/components/DynamicComponent";
import { currentUser } from "@/services/auth";
import HelpTrigger from "@/components/HelpTrigger";

export default function QueriesListEmptyState({ page, searchTerm, selectedTags }) {
  if (searchTerm !== "") {
    return <BigMessage message={window.W_L.find_nothing} icon="fa-search" />;
  }
  if (selectedTags.length > 0) {
    return <NoTaggedObjectsFound objectType="queries" tags={selectedTags} />;
  }
  switch (page) {
    case "favorites":
      return <BigMessage message={window.W_L.my_favorite_query} icon="fa-star" />;
    case "archive":
      return <BigMessage message={window.W_L.archived_query} icon="fa-archive" />;
    case "my":
      const my_msg = currentUser.hasPermission("create_query") ? (
        <span>
          <Link.Button href="queries/new" type="primary" size="small">
            {window.W_L.create_query}
          </Link.Button>{" "}
          <HelpTrigger className="f-13" type="QUERIES" showTooltip={false}>
            {window.W_L.need_help}?
          </HelpTrigger>
        </span>
      ) : (
        <span>{window.W_L.find_nothing}</span>
      );
      return <BigMessage icon="fa-search">{my_msg}</BigMessage>;
    default:
      return (
        <DynamicComponent name="QueriesList.EmptyState">
          <EmptyState
            icon="fa fa-code"
            illustration="query"
            description={window.W_L.get_data_from_datasource}
            helpMessage={<EmptyStateHelpMessage helpTriggerType="QUERIES" />}
          />
        </DynamicComponent>
      );
  }
}

QueriesListEmptyState.propTypes = {
  page: PropTypes.string.isRequired,
  searchTerm: PropTypes.string.isRequired,
  selectedTags: PropTypes.array.isRequired, // eslint-disable-line react/forbid-prop-types
};
