import chokidar from 'chokidar';
import { FSWatcher, createReadStream } from 'fs';
import path from 'path';

import { CubeCloudClient, DeployDirectory } from '@cubejs-backend/cloud';

// TODO: use AuthObject from @cubejs-backend/cloud
type AuthObject = {
  auth: string,
  url?: string,
  deploymentId?: string,
  deploymentUrl: string
};

export class LivePreviewWatcher {
  private watcher: FSWatcher;

  private handleQueueTimeout: any;

  private cubeCloudClient = new CubeCloudClient();

  private auth: AuthObject;

  private queue: {}[] = [];

  public setAuth(token: string) {
    try {
      const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString());
      this.auth = {
        auth: token,
        deploymentId: payload.d,
        deploymentUrl: payload.dUrl,
        url: payload.url,
      };
    } catch (error) {
      console.error(error);
    }
  }

  public startWatch(): void {
    if (!this.auth) throw new Error('Auth isn\'t set');
    if (!this.watcher) {
      const { deploymentUrl } = this.auth;
      console.log(`☁️  Start live-preview with Cube Cloud. Url: ${deploymentUrl}`);
      this.watcher = chokidar.watch(
        process.cwd(),
        {
          ignoreInitial: true,
          ignored: [
            '**/node_modules/**',
            '**/.*'
          ]
        }
      );

      let preSaveTimeout: NodeJS.Timeout;
      this.watcher.on('all', (event, p) => {
        console.log('LivePreviewWatcher:', event, p.replace(process.cwd(), ''));
        if (preSaveTimeout) clearTimeout(preSaveTimeout);

        preSaveTimeout = setTimeout(() => {
          this.queue.push({ time: new Date().getTime() });
        }, 1000);
      });

      this.handleQueue();
    }
  }

  private async handleQueue() {
    try {
      const [job] = this.queue;
      if (job) {
        this.queue = [];
        await this.deploy();
      }
    } catch (error) {
      console.error(error);
    } finally {
      this.handleQueueTimeout = setTimeout(async () => this.handleQueue(), 1000);
    }
  }

  public stopWatch(): void {
    if (this.watcher) {
      this.watcher.close();
      delete this.watcher;
    }

    if (this.handleQueueTimeout) clearTimeout(this.handleQueueTimeout);
  }

  public async deploy(): Promise<Boolean> {
    if (!this.auth) throw new Error('Auth isn\'t set');
    const { auth } = this;
    const directory = process.cwd();

    const deployDir = new DeployDirectory({ directory });
    const fileHashes: any = await deployDir.fileHashes();

    const upstreamHashes = await this.cubeCloudClient.getUpstreamHashes({ auth });
    const { transaction } = await this.cubeCloudClient.startUpload({ auth });

    const files = Object.keys(fileHashes);
    const fileHashesPosix = {};

    for (let i = 0; i < files.length; i++) {
      const file = files[i];

      const filePosix = file.split(path.sep).join(path.posix.sep);
      fileHashesPosix[filePosix] = fileHashes[file];

      if (!upstreamHashes[filePosix] || upstreamHashes[filePosix].hash !== fileHashes[file].hash) {
        console.log('Upload file', filePosix);
        await this.cubeCloudClient.uploadFile({
          auth,
          transaction,
          fileName: filePosix,
          data: createReadStream(path.join(directory, file))
        });
      }
    }
    await this.cubeCloudClient.finishUpload({ transaction, files: fileHashesPosix, auth });

    return true;
  }

  public async getStatus() {
    return {
      // TODO: add http request, get status from dev-mode api
      enabled: !!this.watcher
    };
  }
}
