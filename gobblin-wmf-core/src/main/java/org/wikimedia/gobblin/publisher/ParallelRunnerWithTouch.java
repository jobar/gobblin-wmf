package org.wikimedia.gobblin.publisher;

import java.util.concurrent.locks.Lock;

import org.apache.gobblin.util.ParallelRunner;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.common.util.concurrent.Striped;


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
