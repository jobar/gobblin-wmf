package org.wikimedia.gobblin.publisher;

import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

public class ParallelRunnerWithTouch extends org.apache.gobblin.util.ParallelRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(org.apache.gobblin.util.ParallelRunner.class);

    private final Striped<Lock> locks = Striped.lazyWeakLock(2147483647);

    public ParallelRunnerWithTouch(int threads, FileSystem fs) {
        super(threads, fs);
    }

    public void touchPath(final Path path) {
        this.submitCallable(new Callable<Void>() {
            public Void call() throws Exception {
                Lock lock = locks.get(path.toString());
                lock.lock();
                try {
                    if (getFs().isDirectory(path.getParent())) {
                        if (!getFs().exists(path)) {
                            getFs().create(path).close();
                        }
                    } else {
                        LOGGER.warn(String.format("Failed to touch %s as parent is not an existing folder", path));
                    }
                } catch (FileAlreadyExistsException var7) {
                    LOGGER.warn(String.format("Failed to touch %s as parent is not an existing folder", path), var7);
                } finally {
                    lock.unlock();
                }
                return null;
            }
        }, "Touch path " + path);
    }
}