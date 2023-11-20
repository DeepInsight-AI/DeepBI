import React from "react";
import PropTypes from "prop-types";
import Tooltip from "@/components/Tooltip";
import PlainButton from "@/components/PlainButton";
import { localizeTime, durationHumanize } from "@/lib/utils";
import { RefreshScheduleType, RefreshScheduleDefault } from "../proptypes";

import "./ScheduleDialog.css";

export default class SchedulePhrase extends React.Component {
  static propTypes = {
    schedule: RefreshScheduleType,
    isNew: PropTypes.bool.isRequired,
    isLink: PropTypes.bool,
    onClick: PropTypes.func,
  };

  static defaultProps = {
    schedule: RefreshScheduleDefault,
    isLink: false,
    onClick: () => {},
  };

  get content() {
    const { interval: seconds } = this.props.schedule || SchedulePhrase.defaultProps.schedule;
    if (!seconds) {
      return [window.W_L.flush_never];
    }
    const humanized = durationHumanize(seconds, {
      omitSingleValueNumber: true, 
    });
    const short = window.W_L.every + `${humanized}`;
    let full = window.W_L.every + `${humanized}`+ window.W_L.auto_flush;

    const { time, day_of_week: dayOfWeek } = this.props.schedule;
    if (time) {
      full += window.W_L.at +`  ${localizeTime(time)}`;
    }
    if (dayOfWeek) {
      full += window.W_L.on + `  ${dayOfWeek}`;
    }

    return [short, full];
  }

  render() {
    if (this.props.isNew) {
      return window.W_L.flush_never;
    }

    const [short, full] = this.content;
    const content = full ? <Tooltip title={full}>{short}</Tooltip> : short;

    return this.props.isLink ? (
      <PlainButton type="link" className="schedule-phrase" onClick={this.props.onClick} data-test="EditSchedule">
        {content}
      </PlainButton>
    ) : (
      content
    );
  }
}
