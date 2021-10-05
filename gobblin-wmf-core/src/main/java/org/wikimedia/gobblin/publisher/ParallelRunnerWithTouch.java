package org.wikimedia.gobblin.publisher;

import com.google.common.util.concurrent.Striped;

import org.apache.gobblin.util.ParallelRunner;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

public class ParallelRunnerWithTouch extends ParallelRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelRunnerWithTouch.class);

    private final Striped<Lock> locks = Striped.lazyWeakLock(2147483647);

    public ParallelRunnerWithTouch(int threads, FileSystem fs) {
        super(threads, fs);
    }

    public void touchPath(final Path path) {
        this.submitCallable(() -> {
            Lock lock = locks.get(path.toString());
            lock.lock();
            try {
                if (getFs().isDirectory(path.getParent())) {
                    if (!getFs().exists(path)) {
                        getFs().create(path).close();
                    }
                } else {
                    LOGGER.warn("Failed to touch {} as parent is not an existing folder", path);
                }
            } catch (FileAlreadyExistsException fileExistsException) {
                LOGGER.warn("Failed to touch {} as parent is not an existing folder", path, fileExistsException);
            } finally {
                lock.unlock();
            }
            return null;
        }, "Touch path " + path);
    }
}