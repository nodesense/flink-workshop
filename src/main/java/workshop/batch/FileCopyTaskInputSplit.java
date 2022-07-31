package workshop.batch;


import org.apache.flink.core.io.InputSplit;

/** Implementation of {@code InputSplit} for copying files. */
public class FileCopyTaskInputSplit implements InputSplit {

    private static final long serialVersionUID = -7621656017747660450L;

    private final FileCopyTask task;
    private final int splitNumber;

    public FileCopyTaskInputSplit(FileCopyTask task, int splitNumber) {
        this.task = task;
        this.splitNumber = splitNumber;
    }

    public FileCopyTask getTask() {
        return task;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }
}