"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const mongoose = require("mongoose");
const pg = require("pg");
mongoose.Promise = Promise;
const pools = {};
function getMongoConnection(uri) {
    return new Promise((resolve) => {
        if (mongoose.connection.readyState === 1) {
            return resolve(mongoose.connection);
        }
        mongoose.connect(uri);
        mongoose.connection.once('open', () => {
            resolve(mongoose.connection);
        });
    });
}
exports.getMongoConnection = getMongoConnection;
function getPostgresConnection(uri, ssl = false) {
    return __awaiter(this, void 0, void 0, function* () {
        const pool = pools[uri] = pools[uri] || new pg.Pool({ ssl, connectionString: uri });
        return new Promise((resolve) => __awaiter(this, void 0, void 0, function* () {
            const client = yield pool.connect();
            resolve(client);
        }));
    });
}
exports.getPostgresConnection = getPostgresConnection;
//# sourceMappingURL=Database.js.map