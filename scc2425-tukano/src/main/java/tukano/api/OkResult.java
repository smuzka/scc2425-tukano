package tukano.api;

public class OkResult<T> implements Result<T> {

    final T result;

    public OkResult(T result) {
        this.result = result;
    }

    @Override
    public boolean isOK() {
        return true;
    }

    @Override
    public T value() {
        return result;
    }

    @Override
    public ErrorCode error() {
        return ErrorCode.OK;
    }

    public String toString() {
        return "(OK, " + value() + ")";
    }
}
