package dao.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Book {
    private Long id;
    private Long categoryId;
    private String bookTitle;
    private List<Author> authors;
    private String publisherName;
}