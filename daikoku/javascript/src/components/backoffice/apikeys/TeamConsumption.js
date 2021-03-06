import React, { Component } from 'react';
import { connect } from 'react-redux';
import * as _ from 'lodash';

import { OtoroshiStatsVizualization } from '../../utils';
import { TeamBackOffice } from '../TeamBackOffice';
import * as Services from '../../../services';
import { t } from '../../../locales';

const TeamConsumptionComponent = ({ currentTeam, currentLanguage }) => {
  const mappers = [
    {
      type: 'DoubleRoundChart',
      label: t('Hits by api/plan', currentLanguage),
      title: t('Hits by api/plan', currentLanguage),
      formatter: (data) =>
        _.sortBy(
          data.reduce((acc, item) => {
            const value = acc.find((a) => a.name === item.apiName) || { count: 0 };
            return [
              ...acc.filter((a) => a.name !== item.apiName),
              { name: item.apiName, count: value.count + item.hits },
            ];
          }, []),
          ['name']
        ),
      formatter2: (data) =>
        _.sortBy(
          data.reduce((acc, item) => {
            const plan = `${item.apiName} - ${item.plan}`;
            const value = acc.find((a) => a.name === plan) || { count: 0 };
            return [
              ...acc.filter((a) => a.name !== plan),
              { name: plan, api: item.apiName, count: value.count + item.hits },
            ];
          }, []),
          ['api']
        ),
      dataKey: 'count',
      parentKey: 'api',
    },
  ];

  return (
    <TeamBackOffice
      tab="ApiKeys"
      title={`${currentTeam.name} - ${t('Consumption', currentLanguage)}`}>
      <div className="row">
        <div className="col">
          <h1>Consumption</h1>
          <OtoroshiStatsVizualization
            sync={() => Services.syncTeamBilling(currentTeam._id)}
            fetchData={(from, to) =>
              Services.getTeamConsumptions(currentTeam._id, from.valueOf(), to.valueOf())
            }
            mappers={mappers}
            currentLanguage={currentLanguage}
          />
        </div>
      </div>
    </TeamBackOffice>
  );
};

const mapStateToProps = (state) => ({
  ...state.context,
});

export const TeamConsumption = connect(mapStateToProps)(TeamConsumptionComponent);
