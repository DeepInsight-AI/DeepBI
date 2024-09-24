import asyncio

from ai.backend.util.db.auto_process.tools_sd_budget_rules import BudgetRulesToolsSD
from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.db_amazon.generate_tools import ask_question



class GenBudgetRuleSD(BudgetRulesToolsSD):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

    # 新增广告系列
    def create_budget_rules(self,endDate,startTime,endTime,ruleType,budgetIncrease,name,metricName,comparisonOperator,threshold,DaySchedule='False'):
        today = datetime.today()
        # 格式化输出
        startDate = today.strftime('%Y%m%d')
        if ruleType in ('PERFORMANCE'):
            campaigninfo = {
                "budgetRulesDetails": [
                    {
                        "duration": {
                            "dateRangeTypeRuleDuration": {
                                "endDate": endDate,
                                "startDate": startDate
                            }
                        },
                        "recurrence": {
                            "type": "DAILY"
                        },
                        "ruleType": ruleType,
                        "budgetIncreaseBy": {
                            "type": "PERCENT",
                            "value": budgetIncrease
                        },
                        "name": name,
                        "performanceMeasureCondition": {
                            "metricName": metricName,
                            "comparisonOperator": comparisonOperator,
                            "threshold": threshold
                        }
                    }
                ]
            }
        else:
            if DaySchedule in ['True']:
                campaigninfo = {
        "budgetRulesDetails": [
            {
              "duration": {
                "dateRangeTypeRuleDuration": {
                  "endDate": endDate,
                  "startDate": startDate
                }
              },
              "recurrence": {
                "intraDaySchedule": [
                  {
                    "startTime": startTime,
                    "endTime": endTime
                  }
                ],
                "type": "DAILY"
              },
              "ruleType": ruleType,
              "budgetIncreaseBy": {
                "type": "PERCENT",
                "value": budgetIncrease
              },
              "name": name
            }
          ]
        }
            else:
                campaigninfo = {
                    "budgetRulesDetails": [
                        {
                            "duration": {
                                "dateRangeTypeRuleDuration": {
                                    "endDate": endDate,
                                    "startDate": startDate
                                }
                            },
                            "recurrence": {
                                "type": "DAILY"
                            },
                            "ruleType": ruleType,
                            "budgetIncreaseBy": {
                                "type": "PERCENT",
                                "value": budgetIncrease
                            },
                            "name": name
                        }
                    ]
                }
        # 执行创建
        res = self.create_budget_rules_api(campaigninfo)

        # 根据创建结果更新log
        # dbNewTools = DbNewSpTools(self.brand,market)
        # if res[0] == "success":
        #     dbNewTools.create_sp_campaigin(market,portfolioId,endDate,name,res[1],targetingType,state,startDate,budgetType,budget,"success",datetime.now(),"SP",None)
        # else:
        #     dbNewTools.create_sp_campaigin(market,portfolioId,endDate,name,res[1],targetingType,state,startDate,budgetType,budget,"failed",datetime.now(),"SP",None)

        return res[1]


if __name__ == "__main__":
    ins = GenBudgetRuleSD('LAPASA')
    res = ins.create_budget_rules('US',None,'08:00:00','18:00:00','SCHEDULE',30,'test',None,None,None,True)
    print(res)

