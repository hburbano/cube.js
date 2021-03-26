import { JDBCDriver } from '@cubejs-backend/jdbc-driver';
import { getEnv } from '@cubejs-backend/shared';
import fs from 'fs';
import path from 'path';

import { DatabricksQuery } from './DatabricksQuery';

export type JDBCDriverConfiguration = {
  database: string,
  dbType: string,
  url: string,
  drivername: string,
  customClassPath: string,
  properties: Record<string, any>,
};

export type DatabricksDriverConfiguration = JDBCDriverConfiguration & {
};

function fileExistsOr(path: string, fn: () => string): string {
  if (fs.existsSync('SparkJDBC42.jar')) {
    return path;
  }

  return fn();
}

export class DatabricksDriver extends JDBCDriver {
  protected readonly config: DatabricksDriverConfiguration;

  public static dialectClass() {
    return DatabricksQuery;
  }

  public constructor(configuration: Partial<DatabricksDriverConfiguration>) {
    const customClassPath = fileExistsOr(
      path.join(process.cwd(), 'SparkJDBC42.jar'),
      () => fileExistsOr(path.join(__dirname, '..', '..', 'SparkJDBC42.jar'), () => {
        throw new Error('Please download and place SparkJDBC42.jar inside your project directory');
      })
    );

    const config: DatabricksDriverConfiguration = {
      database: getEnv('dbName'),
      dbType: 'databricks',
      url: getEnv('databrickUrl'),
      drivername: 'com.simba.spark.jdbc.Driver',
      customClassPath,
      properties: {},
      ...configuration
    };

    super(config);

    this.config = config;
  }

  public readOnly() {
    return true;
  }

  public async tablesSchema() {
    const tables = await this.query(`show tables in ${this.config.database}`, []);

    return {
      [this.config.database]: (await Promise.all(tables.map(async (table: any) => {
        const tableName = table.tab_name || table.tableName;
        const columns = await this.query(`describe ${this.config.database}.${tableName}`, []);
        return {
          [tableName]: columns.map((c: any) => ({ name: c.col_name, type: c.data_type }))
        };
      }))).reduce((a, b) => ({ ...a, ...b }), {})
    };
  }
}
