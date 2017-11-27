/// <reference types="mongoose" />
import * as mongoose from 'mongoose';
import * as pg from 'pg';
export declare function getMongoConnection(uri: string): Promise<mongoose.Connection>;
export declare function getPostgresConnection(uri: string, ssl?: boolean): Promise<pg.Client>;
