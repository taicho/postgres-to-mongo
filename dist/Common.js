"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
function camelize(str) {
    return str.replace(/(?:^\w|[A-Z]|\b\w|\s+)/g, (match, index) => {
        if (+match === 0) {
            return '';
        }
        return index === 0 ? match.toLowerCase() : match.toUpperCase();
    });
}
exports.camelize = camelize;
function toMongoName(name) {
    if (name.includes('_')) {
        return this.camelize(name.replace(/_/g, ' '));
    }
    else {
        return name;
    }
}
exports.toMongoName = toMongoName;
//# sourceMappingURL=Common.js.map