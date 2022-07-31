package workshop.batch;

import org.apache.flink.core.fs.Path;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/** A Java POJO that represents a task for copying a single file. */
public class FileCopyTask implements Serializable {

    private static final long serialVersionUID = -8760082278978316032L;

    private final Path path;
    private final String relativePath;

    public FileCopyTask(Path path, String relativePath) {
        if (StringUtils.isEmpty(relativePath)) {
            throw new IllegalArgumentException("Relative path should not be empty for: " + path);
        }
        this.path = path;
        this.relativePath = relativePath;
    }

    public Path getPath() {
        return path;
    }

    public String getRelativePath() {
        return relativePath;
    }

    @Override
    public String toString() {
        return "FileCopyTask{" + "path=" + path + ", relativePath='" + relativePath + '\'' + '}';
    }
}