import * as mongoose from 'mongoose';
import * as pg from 'pg';
import { clearLine } from 'readline';

(mongoose as any).Promise = Promise;
const pools: any = {
};

export function getMongoConnection(uri: string) {
    return new Promise<any>((resolve) => {
        if (mongoose.connection.readyState === 1) {
            return resolve(mongoose.connection);
        }
        mongoose.connect(uri, { useMongoClient: true });
        mongoose.connection.once('open', () => {
            resolve(mongoose.connection);
        });
    });
}

export async function getPostgresConnection(uri: string, ssl = false) {
    const pool = pools[uri] = pools[uri] || new pg.Pool({ ssl, connectionString: uri });
    return new Promise<any>(async (resolve) => {
        const client = await pool.connect();
        resolve(client);
    });
}
