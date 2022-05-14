package Model;

import lombok.*;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Employee implements Serializable {
    private String id;
    private String name;
    private String phone;
    private String salary;
    private String age;
    private String departement;
}
