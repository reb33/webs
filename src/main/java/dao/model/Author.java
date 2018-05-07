package dao.model;

import lombok.Data;

@Data
public class Author {
    private Long id;
    private Long bookId;
    private String firstName;
    private String lastName;
}
