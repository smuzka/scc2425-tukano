package tukano.pojoModels;

public class Id {
    private String id;

    public Id() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return id;
    }
}