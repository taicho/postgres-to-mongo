const del = require('del');
const gulp = require('gulp');
const exec = require('child_process').exec;
const tslint = require('gulp-tslint');
const mocha = require('gulp-mocha');
gulp.task('tslint', () =>
    gulp.src('./src/**/*.ts')
        .pipe(tslint({
            formatter: 'verbose'
        }))
        .pipe(tslint.report({ emitError: false }))
);

gulp.task('clean', function () {
    return del(['./dist']);
});
// Basic build process for TS.
gulp.task('build', gulp.series('clean', 'tslint', function (cb) {
    exec('tsc --project ./tsconfig.json', (err, stdout, stderr) => {
        console.log(stdout);
        console.log(stderr);
        cb();
    });
}));

gulp.task('watch', gulp.series('build', function (done) {
    gulp.watch('./src/**/*', gulp.series('build'));
    done();
}));
gulp.task('test', gulp.series('build', function (done) {
    return gulp.src('test/**/*.ts')
        .pipe(mocha({
            reporter: 'spec',
            require: ['ts-node/register'],
            slow: 0,
        })).once('error', (error) => {            
            done();
            process.exit(1);
        })
        .once('end', () => {
            done();
            process.exit();
        });
}));