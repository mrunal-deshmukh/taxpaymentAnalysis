import java.io.Serializable;

public class CityAndData implements Serializable {
    private String city;
    private String state;

    public CityAndData(String city, String state) {
        this.city = city;
        this.state = state;
    }

    public String getCity() {
        return city;
    }

    public String getState() {
        return state;
    }
}
