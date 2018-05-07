package dao.model;

import lombok.Data;

import java.util.List;

@Data
public class Book {
    private Long id;
    private Long categoryId;
    private String bookTitle;
    private List<Author> authors;
    private String publisherName;
}