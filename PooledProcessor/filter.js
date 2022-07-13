const { Transform } = require('stream');

moodule.exports = class IndexFilter extends Transform {

    constructor(index) {
        super({
            readableObjectMode: true,
            writableObjectMode: true
        })
        this.filterIndex = index;
    }

    _transform(obj, encoding, next) {
        if (obj.value.index == this.filterIndex) {
            return next(null, chunk);
        }
        next();
    }
}