'use strict';

var async = require('async');
var ravencore = require('@ravendevkit/ravencore-lib');
var BigNumber = require('bignumber.js');
var LRU = require('lru-cache');
var Common = require('../lib/common');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var STATISTIC_TYPE = 'STATISTIC';
var SupplyHelper = require('../helpers/SupplyHelper');
var BN = ravencore.crypto.BN;
var pools = require('../pools.json');
var _ = require('lodash');
/**
 *
 * @param {Object} options
 * @constructor
 */
function StatisticService(options) {
	var self = this;
    this.node = options.node;
    this.statisticDayRepository = options.statisticDayRepository;

    this.addressBalanceService = options.addressBalanceService;
    this.lastBlockRepository = options.lastBlockRepository;

    /**
     * 24h Cache
     */
    this.subsidyByBlockHeight = LRU(999999);
    this.blocksByHeight = LRU(999999);
    this.feeByHeight = LRU(999999);
    this.outputsByHeight = LRU(999999);
	  this.difficultyByHeight = LRU(999999);
    this.minedByByHeight = LRU(999999);
		this.netHashByHeight = LRU(999999);

		/**
		 * 1h Cache
		 */
		this.minedByByHeight1h = LRU(999999);
		this.blocksByHeight1h = LRU(999999);



    /**
     * Statistic Cache By Days
     */
    this.statisticByDays = LRU(999999999);
    this.knownBlocks = LRU(999999999);

    this.lastCheckedBlock = 0;

    /**
     *
     * @type {Common}
     */
    this.common = new Common({log: this.node.log});

    this.lastTipHeight = 0;
    this.lastTipInProcess = false;
    this.lastTipTimeout = false;

    this.poolStrings = {};
    pools.forEach(function(pool) {
      pool.searchStrings.forEach(function(s) {
        self.poolStrings[s] = {
          poolName: pool.poolName,
          url: pool.url
        };
      });
    });

}

util.inherits(StatisticService, EventEmitter);

/**
 *
 * @param {Function} callback
 * @return {*}
 */
StatisticService.prototype.start = function (callback) {

    var self = this,
        height = self.node.services.ravend.height;

    return async.waterfall([function (callback) {
        return self.lastBlockRepository.setLastBlockType(STATISTIC_TYPE, 0, function(err) {

            if (err) {

                self.common.log.error('[STATISTICS Service] setLastBlockType Error', err);

                return callback(err)
            }

            self.common.log.info('[STATISTICS Service] LastBlockType set');

            return callback();

        });
    }, function (callback) {
        return self.lastBlockRepository.getLastBlockByType(STATISTIC_TYPE, function(err, existingType) {

            if (err) {

                self.common.log.error('[STATISTICS Service] getLastBlockByType Error', err);

                return callback(err)
            }

            self.lastCheckedBlock = existingType.last_block_number;

            self.common.log.info('[STATISTICS Service] getLastBlockByType set', self.lastCheckedBlock);

            return callback();

        });
    }, function (callback) {

        self.common.log.info('[STATISTICS Service] start upd prev blocks');

        return self.processPrevBlocks(height, function (err) {

            if (err) {
                return callback(err);
            }

            self.common.log.info('[STATISTICS Service] updated prev blocks');

            return callback(err);

        });

    }], function (err) {

        if (err) {
            return callback(err);
        }

        self.node.services.ravend.on('tip', self._rapidProtectedUpdateTip.bind(self));
        self._rapidProtectedUpdateTip(height);

        return callback(err);
    });

};

/**
 *
 * @param {Object} data
 * @param {Function} next
 * @return {*}
 */
StatisticService.prototype.process24hBlock = function (data, next) {

    var self = this,
        block = data.blockJson,
        subsidy = data.subsidy,
        fee = data.fee,
        totalOutputs = data.totalOutputs,
		    difficulty = data.blockJson.difficulty,
        minedBy = data.minedBy,
				netHash = data.netHash,
        currentDate = new Date(),
				currentDateHour = new Date();

    currentDate.setDate(currentDate.getDate() - 1);
		currentDateHour.setHours(currentDateHour.getHours() - 1);

    var minTimestamp = currentDate.getTime() / 1000,
        maxAge = (block.time - minTimestamp) * 1000;

    if (maxAge > 0) {
        self.blocksByHeight.set(block.height, block, maxAge);
        self.subsidyByBlockHeight.set(block.height, subsidy, maxAge);
        self.feeByHeight.set(block.height, fee, maxAge);
        self.outputsByHeight.set(block.height, totalOutputs, maxAge);
		    self.difficultyByHeight.set(block.height, difficulty, maxAge);
        self.minedByByHeight.set(block.height, minedBy, maxAge);
				self.netHashByHeight.set(block.height, netHash, maxAge);
    }
		var minTimestampHour = currentDateHour.getTime() / 1000,
        maxAgeHour = (block.time - minTimestampHour) * 1000;
		if (maxAgeHour > 0) {
		    self.blocksByHeight1h.set(block.height, block, maxAgeHour);
		    self.minedByByHeight1h.set(block.height, minedBy, maxAgeHour);
		}
    return next();

};

/**
 *
 * @param {Number} height
 * @param {Function} next
 * @return {*}
 */
StatisticService.prototype.processPrevBlocks = function (height, next) {

    var self = this,
        dataFlow = {
            blockJson: null
        };

    return async.doDuring(
        function(callback) {

            return self.node.getJsonBlock(height, function (err, blockJson) {

                if (err) {
                    return callback(err);
                }

                dataFlow.blockJson = blockJson;

                return callback();
            });

        },
        function(callback) {

            var block = dataFlow.blockJson,
                currentDate = new Date();

            currentDate.setDate(currentDate.getDate() - 1);

            var minTimestamp = currentDate.getTime() / 1000,
                maxAge = (block.time - minTimestamp) * 1000;

            height--;

            if (maxAge > 0) {
                return async.waterfall([function (callback) {
                    return self._getBlockInfo(block.height, function (err, data) {
                        return callback(err, data);
                    });
                }, function (data, callback) {
                    return self.process24hBlock(data, function (err) {
                        return callback(err);
                    });
                }], function (err) {
                    return callback(err, true);
                });

            } else {
                return callback(null, false);
            }

        },
        function (err) {
            return next(err);
        }
    );

};

/**
 *
 * @param {Number} height
 * @param {Function} next
 * @return {*}
 * @private
 */
StatisticService.prototype._getLastBlocks = function(height, next) {

	var self = this,
    	blocks = [];

    for (var i = self.lastCheckedBlock + 1; i <= height; i++) {
            blocks.push(i);
        }

	     return async.eachSeries(blocks, function (blockHeight, callback) {

        return self.processBlock(blockHeight, function (err) {
            return callback(err);
        });

    }, function (err) {
        return next(err);
    });

};

/**
 *
 * @param {Number} blockHeight
 * @param {Function} next
 * @return {*}
 * @private
 */
StatisticService.prototype._getBlockInfo = function (blockHeight, next) {

    var self = this,
        dataFlow = {
            subsidy: null,
            block: null,
            blockJson: null,
			      fee: 0,
			      totalOutputs: 0,
            minedBy: null,
						netHash: null,
						transaction: null
		};

		return async.waterfall([function (callback) {
        return self.node.getJsonBlock(blockHeight, function (err, blockJson) {
                if((err && err.code === -5) || (err && err.code === -8)) {
                    return callback(err);
                } else if(err) {
                    return callback(err);
                }

                dataFlow.blockJson = blockJson;

                return callback();
          });
        }, function (callback) {

            /**
			         * Block
             */
            return self.node.getBlock(blockHeight, function(err, block) {

                if((err && err.code === -5) || (err && err.code === -8)) {
                    return callback(err);
                } else if(err) {
                    return callback(err);
                }

                dataFlow.block = block;

                return callback();

           });
		}, function (callback) {

            /**
			 * Subsidy
             */
             return self.getBlockReward(blockHeight, function (err, result) {
                 dataFlow.subsidy = result;
                 return callback();
			});

		}, function (callback) {

            /**
			       * Fee
             */



             var transaction0 = dataFlow.block.transactions[0],
                 currentVoutsAmount = 0;

                 transaction0.outputs.forEach(function (output) {
                       currentVoutsAmount += output.satoshis;
                 });

                 if ((currentVoutsAmount - dataFlow.subsidy) > 0) {
                 dataFlow.fee = currentVoutsAmount - dataFlow.subsidy;
                 }

		       return callback();

		}, function (callback) {

            /**
			 * Total outputs
             */

			var trxsExcept = [];

            trxsExcept.push(0);

            dataFlow.block.transactions.forEach(function (transaction, idx) {
                if (trxsExcept.indexOf(idx) === -1) {
                    transaction.outputs.forEach(function (output) {
						dataFlow.totalOutputs += output.satoshis;
                    });
				        }
			      });

            return callback();

		}, function (callback) {

		          /**
			 * networkhashps
		           */

			return self.node.getNetworkHash(blockHeight, function(err, hashps) {

				if((err && err.code === -5) || (err && err.code === -8)) {
						return callback(err);
				} else if(err) {
						return callback(err);
				}

				dataFlow.netHash = hashps;

				return callback();

	 		});

		}, function (callback) {

            if (blockHeight === 0) {
                return callback();
            }

            var txHash;


            txHash = dataFlow.block.transactions[0].hash;


            return self.getDetailedTransaction(txHash, function (err, trx) {

            if (err) {
                    return callback(err);
                }

                dataFlow.transaction = trx;

                return callback();

            });
        }, function (callback) {

            /**
			 * minedBy
             */


             var reward = self.getBlockRewardr(blockHeight);
             dataFlow.transaction.outputs.forEach(function (output) {
                 if (output.satoshis > (reward * 0.8)) {
                     dataFlow.minedBy = output.address;
				 }
             });

            return callback();

		}], function (err) {

			if (err) {
            return next(err);
			}

        return next(err, dataFlow);

    });

};

/**
 *
 * @param {Number} blockHeight
 * @param {Function} next
 * @return {*}
 */
StatisticService.prototype.processBlock = function (blockHeight, next) {

    var self = this;

    return self._getBlockInfo(blockHeight, function (err, data) {

        if (err) {
            return next(err);
        }

            if (self.knownBlocks.get(blockHeight)) {
                return callback();
            }

            self.knownBlocks.set(blockHeight, true);

            self.lastCheckedBlock = blockHeight;

        var block = data.blockJson,
            date = new Date(block.time * 1000),
                formattedDate = self.formatTimestamp(date);

        return async.waterfall([function (callback) {
            return self.lastBlockRepository.updateOrAddLastBlock(block.height, STATISTIC_TYPE, function (err) {
                return callback(err);
		        });
        }, function (callback) {
            return self.updateOrCreateDay(formattedDate, data, function (err) {
                return callback(err);
	     });
        }, function (callback) {
            return self.process24hBlock(data, function (err) {
                return callback(err);
            });
        }], function (err) {
            return next(err);
        });

    });

};

/**
 *
 * @param {String} date e.g. 01-01-2018
 * @param {Object} data
 * @param next
 * @return {*}
 */
StatisticService.prototype.updateOrCreateDay = function (date, data, next) {

    var self = this,
        block = data.blockJson,
        subsidy = data.subsidy,
        fee = data.fee,
        totalOutputs = data.totalOutputs,
        mined = data.minedBy,
				netHash = data.netHash,
        dataFlow = {
        day: null,
        formattedDay: null
    };

    return async.waterfall([function (callback) {
        return self.statisticDayRepository.getDay(new Date(date), function (err, day) {

            if (err) {
                return callback(err);
            }

            if (!day) {

                dataFlow.day = {
                    totalTransactionFees: {
                        sum: '0',
                        count: '0'
                    },
                    numberOfTransactions: {
                        count: '0'
                    },
                    totalOutputVolume: {
                        sum: '0'
                    },
                    totalBlocks: {
                        count: '0'
                    },
                    difficulty: {
                        sum: []
                    },
                    supply: {
                        sum: '0'
                    },
                    poolData: {
                        pool: []
                    },
										netHash: {
                        sum: '0',
												count: '0'
										},
                    date: date
                };

            } else {
                dataFlow.day = day;
            }

            return callback();

        });

    }, function (callback) {

        var dayBN = self._toDayBN(dataFlow.day);

        dayBN.totalTransactionFees.sum = dayBN.totalTransactionFees.sum.plus(fee.toString());
        dayBN.totalTransactionFees.count = dayBN.totalTransactionFees.count.plus(1);

        dayBN.totalBlocks.count = dayBN.totalBlocks.count.plus(1);

        dayBN.numberOfTransactions.count = dayBN.numberOfTransactions.count.plus(block.tx.length);

        dayBN.totalOutputVolume.sum = dayBN.totalOutputVolume.sum.plus(totalOutputs.toString());


       dayBN.difficulty.sum.push(block.difficulty.toString());

			 dayBN.netHash.sum = dayBN.netHash.sum.plus(netHash.toString());
       dayBN.netHash.count = dayBN.netHash.count.plus(1);


	   var objIndex = dayBN.poolData.pool.findIndex((obj => obj.minedby == mined));

       if (objIndex == -1) {
          dayBN.poolData.pool.push({minedby: mined, count: 1});

       } else {
		  dayBN.poolData.pool[objIndex].count += 1;

       }


       dayBN.supply.sum = SupplyHelper.getTotalSupplyByHeight(block.height).mul(1e8);


        return self.statisticDayRepository.createOrUpdateDay(new Date(date), dayBN, function (err) {
            return callback(err);
        });

    }], function (err) {
        return next(err);
    });

};

/**
 *
 * @param {Object} day
 * @return {{totalTransactionFees: {sum, count}, numberOfTransactions: {count}, totalOutputVolume: {sum}, totalBlocks: {count}, difficulty: {sum, count}, stake: {sum}, supply: {sum}, date}}
 * @private
 */
StatisticService.prototype._toDayBN = function (day) {
    return {
        totalTransactionFees: {
            sum: new BigNumber(day.totalTransactionFees.sum),
            count: new BigNumber(day.totalTransactionFees.count)
        },
        numberOfTransactions: {
            count: new BigNumber(day.numberOfTransactions.count)
        },
        totalOutputVolume: {
            sum: new BigNumber(day.totalOutputVolume.sum)
        },
        totalBlocks: {
            count: new BigNumber(day.totalBlocks.count)
        },
        difficulty: {
            sum: day.difficulty.sum
        },
        supply: {
            sum: new BigNumber(day.supply.sum)
        },
        poolData: {
            pool: day.poolData.pool
        },
				netHash: {
						sum: new BigNumber(day.netHash.sum),
						count: new BigNumber(day.netHash.count)
				},
        date: day.date
    };
};

/**
 * helper to convert timestamps to yyyy-mm-dd format
 * @param {Date} date
 * @returns {string} yyyy-mm-dd format
 */
StatisticService.prototype.formatTimestamp = function(date) {
    var yyyy = date.getUTCFullYear().toString();
    var mm = (date.getUTCMonth() + 1).toString(); // getMonth() is zero-based
    var dd = date.getUTCDate().toString();

    return yyyy + '-' + (mm[1] ? mm : '0' + mm[0]) + '-' + (dd[1] ? dd : '0' + dd[0]); //padding
};

/**
 *
 * @param {number} height
 * @returns {boolean}
 * @private
 */
StatisticService.prototype._rapidProtectedUpdateTip = function(height) {

    var self = this;

    if (height > this.lastTipHeight) {
        this.lastTipHeight = height;
    }

    if (this.lastTipInProcess || height < this.lastCheckedBlock) {
        return false;
    }

    this.lastTipInProcess = true;

    self.common.log.info('[STATISTICS Service] start upd from ', self.lastCheckedBlock + 1 , ' to ', height);

    return this._getLastBlocks(height, function (err) {

        self.lastTipInProcess = false;

        if (err) {
            return false;
        }

        self.emit('updated', {height: height});

        self.common.log.info('[STATISTICS Service] updated to ', height);

        if (self.lastTipHeight !== height) {
            self._rapidProtectedUpdateTip(self.lastTipHeight);
        }

    });

};

/**
 *
 * @param {Number} days
 * @param {Function} next
 * @return {*}
 */
StatisticService.prototype.getStats = function (days, next) {

    var self = this,
        currentDate = new Date(),
        formattedDate = this.formatTimestamp(currentDate),
        from = new Date(formattedDate);

    from.setDate(from.getDate() - days);

    return self.statisticDayRepository.getStats(from, new Date(formattedDate), function (err, stats) {
        return next(err, stats);
    });

};

StatisticService.prototype.getStatsByDate = function (date, next) {

    var self = this;

    return self.statisticDayRepository.getDay(date, function (err, stats) {
        return next(err, stats);
    });

};
/**
 *
 * @param {Number} days
 * @param {Function} next
 */
StatisticService.prototype.getDifficulty = function (days, next) {

    var self = this;

    return self.getStats(days, function (err, stats) {

        if (err) {
            return next(err);
        }

        var results = [];
        var diffMode = [];
        var sumDiff = 0;


        stats.forEach(function (day) {

            diffMode = self.mode(day.difficulty.sum);

            if (diffMode.length-1 > 1) {
    			       sumDiff = diffMode[diffMode.length-1].toString();
    		    } else {
    			       sumDiff = diffMode[0].toString();
    		    }

            results.push({
                date: self.formatTimestamp(day.date),
                sum: sumDiff
            });

        });

        return next(err, results);

    });

};

StatisticService.prototype.getNetHash = function (days, next) {

    var self = this;

    return self.getStats(days, function (err, stats) {

        if (err) {
            return next(err);
        }

        var results = [];

        stats.forEach(function (day) {

            results.push({
                date: self.formatTimestamp(day.date),
                sum: day.netHash.sum > 0 && day.netHash.count > 0 ? new BigNumber(day.netHash.sum).dividedBy(day.netHash.count).toNumber() : 0
            });

        });

        return next(err, results);

    });

};

StatisticService.prototype.getPools = function (date, next) {

    var self = this;

    return self.getStatsByDate(date, function (err, stats) {

        if (err) {
            return next(err);
        }
		var results = null;
		if (stats) {
		var totalBlocks = parseInt(stats.totalBlocks.count);
		var poolsarr = JSON.parse(JSON.stringify(stats.poolData.pool));
		poolsarr.forEach(function(obj) {
			var name;
			name = self.getPoolInfo(obj.minedby);
			if (_.isEmpty(name)) {
				obj.address = obj.minedby;
				obj.poolName = "Unknown",
				obj.url = "";
 		    } else {
			 	obj.address = obj.minedby;
				obj.poolName = name.poolName;
				obj.url = name.url;
			}
			delete obj.minedby;
			var blocksWon = parseInt(obj.count);
			var tempnum = blocksWon / totalBlocks * 100;
			obj.blocks_found = obj.count;
			obj.percent_total = tempnum.toFixed(2);
			delete obj.count;

		});
        poolsarr.sort(function(a,b){
			return b.percent_total - a.percent_total;
		});
		results = {
                date: self.formatTimestamp(stats.date),
				block_count: totalBlocks,
                Pools: poolsarr
            };
		}


        return next(err, results);

    });

};
/**
 *
 * @param {Number} days
 * @param {Function} next
 */
StatisticService.prototype.getSupply = function (days, next) {

    var self = this;

    return self.getStats(days, function (err, stats) {

        if (err) {
            return next(err);
        }

        var results = [];

        stats.forEach(function (day) {

            var sumBN = new BigNumber(day.supply.sum);

            results.push({
                date: self.formatTimestamp(day.date),
                sum: sumBN.gt(0) ? sumBN.dividedBy(1e8).toString(10) : '0'
            });

        });

        return next(err, results);

    });

};

/**
 *
 * @param {Number} days
 * @param {Function} next
 */
StatisticService.prototype.getOutputs = function (days, next) {

    var self = this;

    return self.getStats(days, function (err, stats) {

        if (err) {
            return next(err);
        }

        var results = [];

        stats.forEach(function (day) {

			var outputBN = new BigNumber(day.totalOutputVolume.sum);

            results.push({
                date: self.formatTimestamp(day.date),
                sum: day.totalOutputVolume && day.totalOutputVolume.sum > 0 ? outputBN.dividedBy(1e8).toFixed(8) : 0
            });

        });

        return next(err, results);

    });

};

/**
 *
 * @param {Number} days
 * @param {Function} next
 */
StatisticService.prototype.getTransactions = function (days, next) {

    var self = this;

    return self.getStats(days, function (err, stats) {

        if (err) {
            return next(err);
        }

        var results = [];

        stats.forEach(function (day) {

            results.push({
                date: self.formatTimestamp(day.date),
                transaction_count: parseInt(day.numberOfTransactions.count),
                block_count: parseInt(day.totalBlocks.count)
            });

        });

        return next(err, results);

    });

};

/**
 *
 * @param {Number} days
 * @param {Function} next
 */
StatisticService.prototype.getFees = function (days, next) {

    var self = this;

    return self.getStats(days, function (err, stats) {

        if (err) {
            return next(err);
        }

        var results = [];

        stats.forEach(function (day) {

            var avg = day.totalTransactionFees.sum > 0 && day.totalTransactionFees.count > 0 ? new BigNumber(day.totalTransactionFees.sum).dividedBy(day.totalTransactionFees.count).toNumber() : 0;

            results.push({
                date: self.formatTimestamp(day.date),
                fee: (avg / 1e8).toFixed(8)
            });

        });

        return next(err, results);

    });

};

/**
 *
 * @param {Function} nextCb
 * @return {*}
 */
StatisticService.prototype.getTotal = function(nextCb) {

    var self = this,
        initHeight = self.lastCheckedBlock,
        height = initHeight,
        next = true,
        sumBetweenTime = 0,
        countBetweenTime = 0,
        numTransactions = 0,
        minedBlocks = 0,
        minedCurrencyAmount = 0,
        allFee = 0,
        sumDifficulty = [],
        totalOutputsAmount = 0,
        dayPoolData = [],
				sumNetHash = 0,
				countNetHash = 0;

    while(next && height > 0) {

        var currentElement = self.blocksByHeight.get(height),
            subsidy = self.subsidyByBlockHeight.get(height),
            outputAmount = self.outputsByHeight.get(height),
			      difficulty = self.difficultyByHeight.get(height),
            mined = self.minedByByHeight.get(height),
						netHash = self.netHashByHeight.get(height);
        if (currentElement) {

            var nextElement = self.blocksByHeight.get(height + 1),
                fee = self.feeByHeight.get(height);

            if (nextElement) {
                sumBetweenTime += (nextElement.time - currentElement.time);
                countBetweenTime++;
            }

            numTransactions += currentElement.tx.length;
            minedBlocks++;


            if (difficulty) {
				        difficulty = JSON.parse(JSON.stringify(difficulty));
                sumDifficulty.push(difficulty.toString());
            }

            if (subsidy) {
                minedCurrencyAmount += subsidy;
            }

            if (fee) {
                allFee += fee;
            }

						if (netHash) {
							sumNetHash += netHash;
							countNetHash++;
            }

						if (outputAmount) {
                totalOutputsAmount += outputAmount;
            }


            if (mined) {
              var objIndex = dayPoolData.findIndex((obj => obj.minedby == mined));

                if (objIndex == -1) {
                   dayPoolData.push({minedby: mined, count: 1});

                } else {
         		       dayPoolData[objIndex].count += 1;

                }

            }

        } else {
            next = false;
        }

        height--;

    }

    var totDiff = 0;
    var totDiffMode = [];
    totDiffMode = self.mode(sumDifficulty);

    if (totDiffMode.length-1 > 1) {
         totDiff = totDiffMode[totDiffMode.length-1].toString();
    } else {
         totDiff = totDiffMode[0];
    }
		var poolsArr = JSON.parse(JSON.stringify(dayPoolData));
		poolsArr.forEach(function(obj) {
			var name;
			name = self.getPoolInfo(obj.minedby);
			if (_.isEmpty(name)) {
				obj.address = obj.minedby;
				obj.poolName = "Unknown",
				obj.url = "";
			} else {
				obj.address = obj.minedby;
				obj.poolName = name.poolName;
				obj.url = name.url;
			}
			delete obj.minedby;
			var blocksWon = parseInt(obj.count);
			var tempNum = blocksWon / minedBlocks * 100;
			obj.blocks_found = obj.count;
			obj.percent_total = tempNum.toFixed(2);
			delete obj.count;

		});
		poolsArr.sort(function(a,b){
			return b.percent_total - a.percent_total;
		});
    var result = {
            n_blocks_mined: minedBlocks,
            time_between_blocks: sumBetweenTime && countBetweenTime ? sumBetweenTime / countBetweenTime : 0,
            mined_currency_amount: minedCurrencyAmount,
            transaction_fees: allFee,
            number_of_transactions: numTransactions,
            outputs_volume: totalOutputsAmount,
            difficulty: totDiff,
						network_hash_ps: sumNetHash && countNetHash ? sumNetHash / countNetHash : 0,
            blocks_by_pool: poolsArr
        };

    return nextCb(null, result);

};

StatisticService.prototype.getPoolsLastHour = function(nextCb) {

    var self = this,
        initHeight = self.lastCheckedBlock,
        height = initHeight,
        next = true,
        minedBlocks = 0,
        dayPoolData = [];

    while(next && height > 0) {

        var currentElement = self.blocksByHeight1h.get(height),
            mined = self.minedByByHeight1h.get(height);
        if (currentElement) {

            minedBlocks++;

            if (mined) {
              var objIndex = dayPoolData.findIndex((obj => obj.minedby == mined));

                if (objIndex == -1) {
                   dayPoolData.push({minedby: mined, count: 1});

                } else {
         		       dayPoolData[objIndex].count += 1;

                }

            }

        } else {
            next = false;
        }

        height--;

    }

		var poolsArr = JSON.parse(JSON.stringify(dayPoolData));
		poolsArr.forEach(function(obj) {
			var name;
			name = self.getPoolInfo(obj.minedby);
			if (_.isEmpty(name)) {
				obj.address = obj.minedby;
				obj.poolName = "Unknown",
				obj.url = "";
			} else {
				obj.address = obj.minedby;
				obj.poolName = name.poolName;
				obj.url = name.url;
			}
			delete obj.minedby;
			var blocksWon = parseInt(obj.count);
			var tempNum = blocksWon / minedBlocks * 100;
			obj.blocks_found = obj.count;
			obj.percent_total = tempNum.toFixed(2);
			delete obj.count;

		});
		poolsArr.sort(function(a,b){
			return b.percent_total - a.percent_total;
		});
    var result = {
            n_blocks_mined: minedBlocks,
            blocks_by_pool: poolsArr
        };

    return nextCb(null, result);

};
StatisticService.prototype.getBlockReward = function(height, callback) {
  var halvings = Math.floor(height / 2100000);
  // Force block reward to zero when right shift is undefined.
  if (halvings >= 64) {
    return 0;
  }

  // Subsidy is cut in half every 2,100,000 blocks which will occur approximately every 4 years.
  var subsidy = new BN(5000 * 1e8);
  subsidy = subsidy.shrn(halvings);
  var sub;
  sub = parseInt(subsidy.toString(10));
  callback(null, sub);
};

StatisticService.prototype.getBlockRewardr = function(height) {
  var halvings = Math.floor(height / 2100000);
  // Force block reward to zero when right shift is undefined.
  if (halvings >= 64) {
    return 0;
  }

  // Subsidy is cut in half every 2,100,000 blocks which will occur approximately every 4 years.
  var subsidy = new BN(5000 * 1e8);
  subsidy = subsidy.shrn(halvings);

  return parseInt(subsidy.toString(10));
};

StatisticService.prototype.getPoolInfo = function(paddress) {

  for(var k in this.poolStrings) {
		if (paddress) {
    if (paddress.toString().match(k)) {
      this.poolStrings[k].address = paddress;
	  return this.poolStrings[k];
    }
		}
  }

  return {};
};

/**
 *
 * @return {BigNumber} supply - BigNumber representation of total supply
 */
StatisticService.prototype.getTotalSupply  = function() {
    var blockHeight = this.node.services.ravend.height;

    var supply = (new BigNumber(0)).plus((blockHeight) * 5000);

    return supply;
};
StatisticService.prototype.mode = function(array) {
		if (!array.length) return [];
		var modeMap = {},
			maxCount = 0,
			modes = [];

		array.forEach(function(val) {
			if (!modeMap[val]) modeMap[val] = 1;
			else modeMap[val]++;

			if (modeMap[val] > maxCount) {
				modes = [val];
				maxCount = modeMap[val];
			}
			else if (modeMap[val] === maxCount) {
				modes.push(val);
				maxCount = modeMap[val];
			}
		});
		return modes;
};
StatisticService.prototype.getDetailedTransaction = function(txid, callback) {

    var self = this;
    var tx = null;
    return async.waterfall([function(callback) {

        return self.node.getDetailedTransaction(txid, function(err, transaction) {
            return callback(err, transaction);
        });

    }], function(err, transaction) {
        return callback(err, transaction);
    });

};

module.exports = StatisticService;
