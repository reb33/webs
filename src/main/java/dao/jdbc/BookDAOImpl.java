package dao.jdbc;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import dao.BookDAO;
import dao.model.Author;
import dao.model.Book;
import dao.model.Category;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BookDAOImpl implements BookDAO {
    Connection connection;
    String url = "";

    private Connection createConnection() throws SQLException {
        Properties properties = new Properties();
        try {
            properties.load(this.getClass().getResourceAsStream("connection.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return DriverManager.getConnection(url, properties);
    }


    public List<Book> findAllBooks() {
        List<Book> books = new ArrayList<>();
        try (
                Connection connection = createConnection();
                PreparedStatement booksStatement = connection.prepareStatement("select * from book");
                ResultSet rsBooks = booksStatement.executeQuery();
                PreparedStatement authorsStatement = connection.prepareStatement("select * from author");
                ResultSet rsAuthors = authorsStatement.executeQuery()
        ) {
            Multimap<Long, Author> authors = ArrayListMultimap.create();
            while (rsAuthors.next()) {
                Long bookId = rsAuthors.getLong("BOOK_ID ");
                authors.put(bookId,
                        new Author(rsAuthors.getLong("ID"),
                                bookId,
                                rsAuthors.getString("FIRST_NAME "),
                                rsAuthors.getString("LAST_NAME")));
            }
            while (rsBooks.next()){
                Long bookId = rsBooks.getLong("ID");
                books.add(
                        new Book(
                                bookId,
                                rsBooks.getLong("CATEGORY_ID"),
                                rsBooks.getString("BOOK_TITLE"),
                                new ArrayList<>(authors.get(bookId)),
                                rsBooks.getString("PUBLISHER")
                ));
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return books;
    }

    public List<Book> searchBooksByKeyword(String keyWord) {
        return null;
    }

    public List<Category> findAllCategories() {
        return null;
    }

    public void insert(Book book) {

    }

    public void update(Book book) {

    }

    public void delete(Long bookId) {

    }
}
