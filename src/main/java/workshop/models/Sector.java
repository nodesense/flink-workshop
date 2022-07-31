package workshop.models;

public class Sector {
    public String company;
    public String industry;
    public String asset;
    public String series;
    public String isbn;

    @Override
    public String toString() {
        return "Sector{" +
                "company='" + company + '\'' +
                ", industry='" + industry + '\'' +
                ", asset='" + asset + '\'' +
                ", series='" + series + '\'' +
                ", isbn='" + isbn + '\'' +
                '}';
    }
}
