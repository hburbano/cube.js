"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DruidQuery = void 0;
const moment_timezone_1 = __importDefault(require("moment-timezone"));
const schema_compiler_1 = require("@cubejs-backend/schema-compiler");
const GRANULARITY_TO_INTERVAL = {
    day: date => `DATE_TRUNC('day', ${date})`,
    week: date => `DATE_TRUNC('week', ${date})`,
    hour: date => `DATE_TRUNC('hour', ${date})`,
    minute: date => `DATE_TRUNC('minute', ${date})`,
    second: date => `DATE_TRUNC('second', ${date})`,
    month: date => `DATE_TRUNC('month', ${date})`,
    year: date => `DATE_TRUNC('year', ${date})`
};
class DruidQuery extends schema_compiler_1.BaseQuery {
    timeGroupedColumn(granularity, dimension) {
        return GRANULARITY_TO_INTERVAL[granularity](dimension);
    }
    convertTz(field) {
        // TODO respect day light saving
        const [hour, minute] = moment_timezone_1.default().tz(this.timezone).format('Z').split(':');
        const minutes = parseInt(hour, 10) * 60 + parseInt(minute, 10);
        if (minutes > 0) {
            return `TIMESTAMPADD(MINUTES, ${minutes}, ${field})`;
        }
        return field;
    }
    subtractInterval(date, interval) {
        return `(${date} + INTERVAL ${interval})`;
    }
    addInterval(date, interval) {
        return `(${date} + INTERVAL ${interval})`;
    }
    timeStampCast(value) {
        return `TIME_PARSE(${value})`;
    }
    timeStampParam() {
        return this.timeStampCast('?');
    }
    nowTimestampSql() {
        return `CURRENT_TIMESTAMP`;
    }
}
exports.DruidQuery = DruidQuery;
//# sourceMappingURL=DruidQuery.js.map