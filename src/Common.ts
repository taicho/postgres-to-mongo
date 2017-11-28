export function camelize(str) {
    return str.replace(/(?:^\w|[A-Z]|\b\w|\s+)/g, (match, index) => {
        if (+match === 0) {
            return '';
        }
        return index === 0 ? match.toLowerCase() : match.toUpperCase();
    });
}

export function toMongoName(name: string) {
    if (name.includes('_')) {
        return this.camelize(name.replace(/_/g, ' '));
    } else {
        return name;
    }
}
