const mongoose = require('mongoose');
const Contact = require('../models/Contact');
const Deal = require('../models/Deal');
const LeadTask = require('../models/LeadTask');
const { getCrmOwnerDashboard } = require('./crmOpsService');

const TASK_OPEN_STATUSES = ['pending', 'in_progress'];

const toCleanString = (value) => String(value || '').trim();
const toObjectIdIfValid = (value) =>
  mongoose.Types.ObjectId.isValid(value) ? new mongoose.Types.ObjectId(value) : null;

const buildScopedFilter = ({ userId = null, companyId = null } = {}, extra = {}) => {
  const conditions = [];
  const normalizedUserId = toObjectIdIfValid(userId);
  const normalizedCompanyId = toObjectIdIfValid(companyId);

  if (normalizedCompanyId) {
    conditions.push({ companyId: normalizedCompanyId });
  } else if (normalizedUserId) {
    conditions.push({ userId: normalizedUserId });
  } else {
    conditions.push({ _id: { $exists: false } });
  }

  if (extra && Object.keys(extra).length > 0) {
    conditions.push(extra);
  }

  if (conditions.length === 0) return {};
  if (conditions.length === 1) return conditions[0];
  return { $and: conditions };
};

const normalizeRows = (rows = [], mapper = (item) => item) =>
  (Array.isArray(rows) ? rows : []).map(mapper);

const safeDate = (value) => {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? null : parsed;
};

const resolveDateRange = ({ from = null, to = null } = {}) => {
  const start = safeDate(from);
  const end = safeDate(to);
  return {
    start,
    end
  };
};

const buildCreatedAtFilter = ({ from = null, to = null } = {}) => {
  const { start, end } = resolveDateRange({ from, to });
  if (!start && !end) return {};
  const filter = {};
  if (start) filter.$gte = start;
  if (end) filter.$lte = end;
  return { createdAt: filter };
};

const getCrmReportsSummary = async ({ userId = null, companyId = null } = {}) => {
  const now = new Date();
  const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const endOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59, 999);
  const scope = { userId, companyId };

  const [
    sourceRows,
    contactOverviewRows,
    responseRows,
    taskRows,
    contactLostReasonRows,
    dealLostReasonRows,
    dealMetricsRows,
    dealStageRows,
    leadScoreBandRows,
    ownerDashboard
  ] = await Promise.all([
    Contact.aggregate([
      { $match: buildScopedFilter(scope) },
      {
        $project: {
          sourceType: { $toLower: { $ifNull: ['$sourceType', 'manual'] } },
          source: {
            $trim: {
              input: {
                $ifNull: ['$source', '']
              }
            }
          },
          status: { $toLower: { $ifNull: ['$status', 'nurturing'] } },
          stage: { $toLower: { $ifNull: ['$stage', 'new'] } },
          whatsappOptInStatus: { $toLower: { $ifNull: ['$whatsappOptInStatus', 'unknown'] } }
        }
      },
      {
        $group: {
          _id: {
            sourceType: '$sourceType',
            source: '$source'
          },
          count: { $sum: 1 },
          qualified: {
            $sum: {
              $cond: [
                {
                  $or: [{ $eq: ['$status', 'qualified'] }, { $eq: ['$stage', 'qualified'] }]
                },
                1,
                0
              ]
            }
          },
          won: {
            $sum: {
              $cond: [
                {
                  $or: [{ $eq: ['$status', 'won'] }, { $eq: ['$stage', 'won'] }]
                },
                1,
                0
              ]
            }
          },
          optedIn: {
            $sum: {
              $cond: [{ $eq: ['$whatsappOptInStatus', 'opted_in'] }, 1, 0]
            }
          }
        }
      },
      { $sort: { count: -1, '_id.sourceType': 1, '_id.source': 1 } }
    ]),
    Contact.aggregate([
      { $match: buildScopedFilter(scope) },
      {
        $facet: {
          totals: [
            {
              $group: {
                _id: null,
                total: { $sum: 1 },
                won: {
                  $sum: {
                    $cond: [{ $eq: [{ $toLower: { $ifNull: ['$status', ''] } }, 'won'] }, 1, 0]
                  }
                },
                qualified: {
                  $sum: {
                    $cond: [{ $eq: [{ $toLower: { $ifNull: ['$status', ''] } }, 'qualified'] }, 1, 0]
                  }
                }
              }
            }
          ],
          byStage: [
            {
              $group: {
                _id: { $toLower: { $ifNull: ['$stage', 'new'] } },
                count: { $sum: 1 }
              }
            },
            { $sort: { count: -1, _id: 1 } }
          ],
          byStatus: [
            {
              $group: {
                _id: { $toLower: { $ifNull: ['$status', 'nurturing'] } },
                count: { $sum: 1 }
              }
            },
            { $sort: { count: -1, _id: 1 } }
          ]
        }
      }
    ]),
    Contact.aggregate([
      {
        $match: buildScopedFilter(scope, {
          lastInboundMessageAt: { $ne: null }
        })
      },
      {
        $project: {
          lastInboundMessageAt: 1,
          lastContactAt: 1,
          awaitingReply: {
            $cond: [
              {
                $or: [
                  { $eq: ['$lastContactAt', null] },
                  { $gt: ['$lastInboundMessageAt', '$lastContactAt'] }
                ]
              },
              1,
              0
            ]
          },
          responseMinutes: {
            $cond: [
              {
                $and: [
                  { $ne: ['$lastContactAt', null] },
                  { $gte: ['$lastContactAt', '$lastInboundMessageAt'] }
                ]
              },
              {
                $divide: [
                  { $subtract: ['$lastContactAt', '$lastInboundMessageAt'] },
                  1000 * 60
                ]
              },
              null
            ]
          }
        }
      },
      {
        $group: {
          _id: null,
          awaitingReplyCount: { $sum: '$awaitingReply' },
          respondedCount: {
            $sum: {
              $cond: [{ $ne: ['$responseMinutes', null] }, 1, 0]
            }
          },
          avgResponseMinutes: { $avg: '$responseMinutes' }
        }
      }
    ]),
    LeadTask.aggregate([
      { $match: buildScopedFilter(scope) },
      {
        $facet: {
          summary: [
            {
              $project: {
                status: { $toLower: { $ifNull: ['$status', 'pending'] } },
                taskType: { $toLower: { $ifNull: ['$taskType', 'follow_up'] } },
                dueAt: 1
              }
            },
            {
              $group: {
                _id: null,
                total: { $sum: 1 },
                completed: {
                  $sum: {
                    $cond: [{ $eq: ['$status', 'completed'] }, 1, 0]
                  }
                },
                overdue: {
                  $sum: {
                    $cond: [
                      {
                        $and: [
                          { $in: ['$status', TASK_OPEN_STATUSES] },
                          { $ne: ['$dueAt', null] },
                          { $lt: ['$dueAt', now] }
                        ]
                      },
                      1,
                      0
                    ]
                  }
                },
                dueToday: {
                  $sum: {
                    $cond: [
                      {
                        $and: [
                          { $in: ['$status', TASK_OPEN_STATUSES] },
                          { $gte: ['$dueAt', startOfDay] },
                          { $lte: ['$dueAt', endOfDay] }
                        ]
                      },
                      1,
                      0
                    ]
                  }
                },
                todayCalls: {
                  $sum: {
                    $cond: [
                      {
                        $and: [
                          { $eq: ['$taskType', 'call'] },
                          { $in: ['$status', TASK_OPEN_STATUSES] },
                          { $gte: ['$dueAt', startOfDay] },
                          { $lte: ['$dueAt', endOfDay] }
                        ]
                      },
                      1,
                      0
                    ]
                  }
                },
                followUpCompleted: {
                  $sum: {
                    $cond: [
                      {
                        $and: [{ $eq: ['$taskType', 'follow_up'] }, { $eq: ['$status', 'completed'] }]
                      },
                      1,
                      0
                    ]
                  }
                },
                followUpTotal: {
                  $sum: {
                    $cond: [{ $eq: ['$taskType', 'follow_up'] }, 1, 0]
                  }
                }
              }
            }
          ],
          byType: [
            {
              $project: {
                taskType: { $toLower: { $ifNull: ['$taskType', 'follow_up'] } },
                status: { $toLower: { $ifNull: ['$status', 'pending'] } }
              }
            },
            {
              $group: {
                _id: '$taskType',
                total: { $sum: 1 },
                completed: {
                  $sum: {
                    $cond: [{ $eq: ['$status', 'completed'] }, 1, 0]
                  }
                }
              }
            },
            { $sort: { total: -1, _id: 1 } }
          ]
        }
      }
    ]),
    Contact.aggregate([
      {
        $match: buildScopedFilter(scope, {
          lostReason: { $nin: [null, ''] }
        })
      },
      {
        $group: {
          _id: '$lostReason',
          count: { $sum: 1 }
        }
      },
      { $sort: { count: -1, _id: 1 } },
      { $limit: 10 }
    ]),
    Deal.aggregate([
      {
        $match: buildScopedFilter(scope, {
          lostReason: { $nin: [null, ''] }
        })
      },
      {
        $group: {
          _id: '$lostReason',
          count: { $sum: 1 }
        }
      },
      { $sort: { count: -1, _id: 1 } },
      { $limit: 10 }
    ]),
    Deal.aggregate([
      { $match: buildScopedFilter(scope) },
      {
        $group: {
          _id: null,
          total: { $sum: 1 },
          open: {
            $sum: {
              $cond: [{ $eq: [{ $toLower: { $ifNull: ['$status', 'open'] } }, 'open'] }, 1, 0]
            }
          },
          won: {
            $sum: {
              $cond: [{ $eq: [{ $toLower: { $ifNull: ['$status', 'open'] } }, 'won'] }, 1, 0]
            }
          },
          lost: {
            $sum: {
              $cond: [{ $eq: [{ $toLower: { $ifNull: ['$status', 'open'] } }, 'lost'] }, 1, 0]
            }
          },
          pipelineValue: {
            $sum: {
              $cond: [
                { $eq: [{ $toLower: { $ifNull: ['$status', 'open'] } }, 'open'] },
                { $ifNull: ['$value', 0] },
                0
              ]
            }
          },
          wonValue: {
            $sum: {
              $cond: [
                { $eq: [{ $toLower: { $ifNull: ['$status', 'open'] } }, 'won'] },
                { $ifNull: ['$value', 0] },
                0
              ]
            }
          }
        }
      }
    ]),
    Deal.aggregate([
      { $match: buildScopedFilter(scope) },
      {
        $group: {
          _id: { $toLower: { $ifNull: ['$stage', 'discovery'] } },
          count: { $sum: 1 },
          value: { $sum: { $ifNull: ['$value', 0] } }
        }
      },
      { $sort: { count: -1, _id: 1 } }
    ]),
    Contact.aggregate([
      { $match: buildScopedFilter(scope) },
      {
        $project: {
          band: {
            $switch: {
              branches: [
                { case: { $gte: ['$leadScore', 60] }, then: 'hot' },
                { case: { $gte: ['$leadScore', 30] }, then: 'warm' }
              ],
              default: 'cold'
            }
          }
        }
      },
      {
        $group: {
          _id: '$band',
          count: { $sum: 1 }
        }
      },
      { $sort: { count: -1, _id: 1 } }
    ]),
    getCrmOwnerDashboard(scope)
  ]);

  const sourceTypeTotals = new Map();
  const topSources = normalizeRows(sourceRows, (row) => ({
    sourceType: toCleanString(row?._id?.sourceType || 'manual') || 'manual',
    source: toCleanString(row?._id?.source || '') || 'Unspecified',
    count: Number(row?.count || 0),
    qualified: Number(row?.qualified || 0),
    won: Number(row?.won || 0),
    optedIn: Number(row?.optedIn || 0)
  }));

  topSources.forEach((item) => {
    const key = item.sourceType;
    const existing = sourceTypeTotals.get(key) || {
      sourceType: key,
      count: 0,
      qualified: 0,
      won: 0,
      optedIn: 0
    };
    existing.count += item.count;
    existing.qualified += item.qualified;
    existing.won += item.won;
    existing.optedIn += item.optedIn;
    sourceTypeTotals.set(key, existing);
  });

  const contactOverview = Array.isArray(contactOverviewRows) ? contactOverviewRows[0] || {} : {};
  const contactTotals = contactOverview?.totals?.[0] || {};
  const taskOverview = Array.isArray(taskRows) ? taskRows[0] || {} : {};
  const taskSummary = taskOverview?.summary?.[0] || {};
  const responseSummary = Array.isArray(responseRows) ? responseRows[0] || {} : {};
  const dealSummary = Array.isArray(dealMetricsRows) ? dealMetricsRows[0] || {} : {};

  return {
    generatedAt: now.toISOString(),
    sources: {
      bySourceType: Array.from(sourceTypeTotals.values()).sort((left, right) => right.count - left.count),
      topSources: topSources.slice(0, 10)
    },
    pipeline: {
      totalContacts: Number(contactTotals.total || 0),
      qualifiedContacts: Number(contactTotals.qualified || 0),
      wonContacts: Number(contactTotals.won || 0),
      qualifiedRate:
        Number(contactTotals.total || 0) > 0
          ? Number(((Number(contactTotals.qualified || 0) / Number(contactTotals.total || 0)) * 100).toFixed(1))
          : 0,
      wonRate:
        Number(contactTotals.total || 0) > 0
          ? Number(((Number(contactTotals.won || 0) / Number(contactTotals.total || 0)) * 100).toFixed(1))
          : 0,
      byStage: normalizeRows(contactOverview?.byStage, (row) => ({
        stage: toCleanString(row?._id || 'new') || 'new',
        count: Number(row?.count || 0)
      })),
      byStatus: normalizeRows(contactOverview?.byStatus, (row) => ({
        status: toCleanString(row?._id || 'nurturing') || 'nurturing',
        count: Number(row?.count || 0)
      })),
      deals: {
        total: Number(dealSummary.total || 0),
        open: Number(dealSummary.open || 0),
        won: Number(dealSummary.won || 0),
        lost: Number(dealSummary.lost || 0),
        pipelineValue: Number(dealSummary.pipelineValue || 0),
        wonValue: Number(dealSummary.wonValue || 0),
        byStage: normalizeRows(dealStageRows, (row) => ({
          stage: toCleanString(row?._id || 'discovery') || 'discovery',
          count: Number(row?.count || 0),
          value: Number(row?.value || 0)
        }))
      }
    },
    owners: {
      summary: ownerDashboard?.summary || {},
      leaderboard: Array.isArray(ownerDashboard?.owners) ? ownerDashboard.owners : []
    },
    response: {
      awaitingReplyCount: Number(responseSummary.awaitingReplyCount || 0),
      respondedCount: Number(responseSummary.respondedCount || 0),
      avgResponseMinutes: Number(Number(responseSummary.avgResponseMinutes || 0).toFixed(1))
    },
    followUps: {
      total: Number(taskSummary.total || 0),
      completed: Number(taskSummary.completed || 0),
      overdue: Number(taskSummary.overdue || 0),
      dueToday: Number(taskSummary.dueToday || 0),
      todayCalls: Number(taskSummary.todayCalls || 0),
      completionRate:
        Number(taskSummary.total || 0) > 0
          ? Number(((Number(taskSummary.completed || 0) / Number(taskSummary.total || 0)) * 100).toFixed(1))
          : 0,
      followUpCompletionRate:
        Number(taskSummary.followUpTotal || 0) > 0
          ? Number(
              (
                (Number(taskSummary.followUpCompleted || 0) / Number(taskSummary.followUpTotal || 0)) *
                100
              ).toFixed(1)
            )
          : 0,
      byType: normalizeRows(taskOverview?.byType, (row) => ({
        taskType: toCleanString(row?._id || 'follow_up') || 'follow_up',
        total: Number(row?.total || 0),
        completed: Number(row?.completed || 0)
      }))
    },
    lostReasons: {
      contacts: normalizeRows(contactLostReasonRows, (row) => ({
        reason: toCleanString(row?._id || 'Unspecified') || 'Unspecified',
        count: Number(row?.count || 0)
      })),
      deals: normalizeRows(dealLostReasonRows, (row) => ({
        reason: toCleanString(row?._id || 'Unspecified') || 'Unspecified',
        count: Number(row?.count || 0)
      }))
    },
    leadScoreBands: normalizeRows(leadScoreBandRows, (row) => ({
      band: toCleanString(row?._id || 'cold') || 'cold',
      count: Number(row?.count || 0)
    }))
  };
};

const getCrmFunnelReport = async ({
  userId = null,
  companyId = null,
  from = null,
  to = null
} = {}) => {
  const summary = await getCrmReportsSummary({ userId, companyId });
  const dateRange = resolveDateRange({ from, to });
  return {
    generatedAt: summary.generatedAt,
    dateRange: {
      from: dateRange.start ? dateRange.start.toISOString() : null,
      to: dateRange.end ? dateRange.end.toISOString() : null
    },
    totals: {
      contacts: Number(summary?.pipeline?.totalContacts || 0),
      qualified: Number(summary?.pipeline?.qualifiedContacts || 0),
      won: Number(summary?.pipeline?.wonContacts || 0)
    },
    conversion: {
      qualifiedRate: Number(summary?.pipeline?.qualifiedRate || 0),
      wonRate: Number(summary?.pipeline?.wonRate || 0)
    },
    stages: Array.isArray(summary?.pipeline?.byStage) ? summary.pipeline.byStage : [],
    statuses: Array.isArray(summary?.pipeline?.byStatus) ? summary.pipeline.byStatus : [],
    sources: Array.isArray(summary?.sources?.topSources) ? summary.sources.topSources : []
  };
};

const getCrmCohortReport = async ({
  userId = null,
  companyId = null,
  from = null,
  to = null,
  granularity = 'month'
} = {}) => {
  const normalizedGranularity = toCleanString(granularity).toLowerCase() === 'week' ? 'week' : 'month';
  const datePattern = normalizedGranularity === 'week' ? '%G-W%V' : '%Y-%m';
  const scope = { userId, companyId };

  const rows = await Contact.aggregate([
    {
      $match: buildScopedFilter(scope, buildCreatedAtFilter({ from, to }))
    },
    {
      $project: {
        cohort: {
          $dateToString: {
            format: datePattern,
            date: { $ifNull: ['$createdAt', '$updatedAt'] }
          }
        },
        status: { $toLower: { $ifNull: ['$status', 'nurturing'] } },
        stage: { $toLower: { $ifNull: ['$stage', 'new'] } },
        whatsappOptInStatus: { $toLower: { $ifNull: ['$whatsappOptInStatus', 'unknown'] } }
      }
    },
    {
      $group: {
        _id: '$cohort',
        contacts: { $sum: 1 },
        qualified: {
          $sum: {
            $cond: [{ $or: [{ $eq: ['$status', 'qualified'] }, { $eq: ['$stage', 'qualified'] }] }, 1, 0]
          }
        },
        won: {
          $sum: {
            $cond: [{ $or: [{ $eq: ['$status', 'won'] }, { $eq: ['$stage', 'won'] }] }, 1, 0]
          }
        },
        optedIn: {
          $sum: {
            $cond: [{ $eq: ['$whatsappOptInStatus', 'opted_in'] }, 1, 0]
          }
        }
      }
    },
    { $sort: { _id: 1 } }
  ]);

  const cohorts = normalizeRows(rows, (row) => ({
    cohort: toCleanString(row?._id || ''),
    contacts: Number(row?.contacts || 0),
    qualified: Number(row?.qualified || 0),
    won: Number(row?.won || 0),
    optedIn: Number(row?.optedIn || 0)
  }));

  return {
    generatedAt: new Date().toISOString(),
    granularity: normalizedGranularity,
    cohorts,
    totals: {
      contacts: cohorts.reduce((sum, item) => sum + item.contacts, 0),
      qualified: cohorts.reduce((sum, item) => sum + item.qualified, 0),
      won: cohorts.reduce((sum, item) => sum + item.won, 0),
      optedIn: cohorts.reduce((sum, item) => sum + item.optedIn, 0)
    }
  };
};

const getCrmOwnerPerformanceReport = async ({ userId = null, companyId = null } = {}) => {
  const scope = { userId, companyId };

  const [contactRows, taskRows, ownerDashboard] = await Promise.all([
    Contact.aggregate([
      { $match: buildScopedFilter(scope) },
      {
        $project: {
          ownerId: {
            $trim: {
              input: { $toString: { $ifNull: ['$ownerId', 'unassigned'] } }
            }
          },
          status: { $toLower: { $ifNull: ['$status', 'nurturing'] } },
          stage: { $toLower: { $ifNull: ['$stage', 'new'] } }
        }
      },
      {
        $group: {
          _id: '$ownerId',
          contactCount: { $sum: 1 },
          qualifiedCount: {
            $sum: {
              $cond: [{ $or: [{ $eq: ['$status', 'qualified'] }, { $eq: ['$stage', 'qualified'] }] }, 1, 0]
            }
          },
          wonCount: {
            $sum: {
              $cond: [{ $or: [{ $eq: ['$status', 'won'] }, { $eq: ['$stage', 'won'] }] }, 1, 0]
            }
          }
        }
      }
    ]),
    LeadTask.aggregate([
      { $match: buildScopedFilter(scope) },
      {
        $project: {
          ownerId: {
            $trim: {
              input: { $toString: { $ifNull: ['$assignedTo', 'unassigned'] } }
            }
          },
          status: { $toLower: { $ifNull: ['$status', 'pending'] } },
          dueAt: 1
        }
      },
      {
        $group: {
          _id: '$ownerId',
          taskCount: { $sum: 1 },
          completedTasks: {
            $sum: {
              $cond: [{ $eq: ['$status', 'completed'] }, 1, 0]
            }
          },
          openTasks: {
            $sum: {
              $cond: [{ $in: ['$status', TASK_OPEN_STATUSES] }, 1, 0]
            }
          }
        }
      }
    ]),
    getCrmOwnerDashboard(scope)
  ]);

  const performanceMap = new Map();
  const ownerLeaderboard = Array.isArray(ownerDashboard?.owners) ? ownerDashboard.owners : [];
  const ownerById = new Map(ownerLeaderboard.map((owner) => [toCleanString(owner?.ownerId), owner]));

  normalizeRows(contactRows, (row) => row).forEach((row) => {
    const ownerId = toCleanString(row?._id || 'unassigned') || 'unassigned';
    performanceMap.set(ownerId, {
      ownerId,
      ownerName: toCleanString(ownerById.get(ownerId)?.ownerName || ownerById.get(ownerId)?.name || ''),
      contactCount: Number(row?.contactCount || 0),
      qualifiedCount: Number(row?.qualifiedCount || 0),
      wonCount: Number(row?.wonCount || 0),
      taskCount: 0,
      completedTasks: 0,
      openTasks: 0
    });
  });

  normalizeRows(taskRows, (row) => row).forEach((row) => {
    const ownerId = toCleanString(row?._id || 'unassigned') || 'unassigned';
    const existing = performanceMap.get(ownerId) || {
      ownerId,
      ownerName: toCleanString(ownerById.get(ownerId)?.ownerName || ownerById.get(ownerId)?.name || ''),
      contactCount: 0,
      qualifiedCount: 0,
      wonCount: 0,
      taskCount: 0,
      completedTasks: 0,
      openTasks: 0
    };
    existing.taskCount = Number(row?.taskCount || 0);
    existing.completedTasks = Number(row?.completedTasks || 0);
    existing.openTasks = Number(row?.openTasks || 0);
    performanceMap.set(ownerId, existing);
  });

  const owners = Array.from(performanceMap.values())
    .map((owner) => ({
      ...owner,
      qualifiedRate:
        owner.contactCount > 0 ? Number(((owner.qualifiedCount / owner.contactCount) * 100).toFixed(1)) : 0,
      wonRate: owner.contactCount > 0 ? Number(((owner.wonCount / owner.contactCount) * 100).toFixed(1)) : 0,
      taskCompletionRate:
        owner.taskCount > 0 ? Number(((owner.completedTasks / owner.taskCount) * 100).toFixed(1)) : 0
    }))
    .sort((left, right) => right.contactCount - left.contactCount);

  return {
    generatedAt: new Date().toISOString(),
    summary: ownerDashboard?.summary || {},
    owners
  };
};

module.exports = {
  getCrmReportsSummary,
  getCrmFunnelReport,
  getCrmCohortReport,
  getCrmOwnerPerformanceReport
};
