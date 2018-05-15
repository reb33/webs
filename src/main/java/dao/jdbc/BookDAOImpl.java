package dao.jdbc;

import com.google.common.collect.ArrayListMultimap;
import dao.BookDAO;
import dao.model.Author;
import dao.model.Book;
import dao.model.Category;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class BookDAOImpl implements BookDAO {
    String url = "jdbc:h2:http://localhost:8082";

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
        List<Book> books;
        try (
                Connection connection = createConnection();
                PreparedStatement booksStatement = connection.prepareStatement("select * from book");
                ResultSet rsBooks = booksStatement.executeQuery();
        ) {
            books = getBooks(connection, rsBooks);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return books;
    }

    private List<Book> getBooks(Connection connection, ResultSet rsBooks) {
        List<Book> books = new ArrayList<>();
        try (
                Statement authorsStatement = connection.createStatement()
        ) {

            while (rsBooks.next()) {
                Long bookId = rsBooks.getLong("ID");
                books.add(
                        new Book(
                                bookId,
                                rsBooks.getLong("CATEGORY_ID"),
                                rsBooks.getString("BOOK_TITLE"),
                                rsBooks.getString("PUBLISHER")
                        ));
            }

            String bookIds = books.stream().map(Book::getId).map(String::valueOf).collect(Collectors.joining(","));
            ArrayListMultimap<Long, Author> authors = ArrayListMultimap.create();
            ResultSet rsAuthors = authorsStatement.executeQuery(String.format("select * from author where BOOK_ID in (%s)", bookIds));
            while (rsAuthors.next()) {
                Long bookId = rsAuthors.getLong("BOOK_ID ");
                authors.put(bookId,
                        new Author(rsAuthors.getLong("ID"),
                                bookId,
                                rsAuthors.getString("FIRST_NAME "),
                                rsAuthors.getString("LAST_NAME")));
            }
            books.forEach(book -> book.setAuthors(authors.get(book.getId())));

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return books;
    }

    public List<Book> searchBooksByKeyword(String keyWord) {
        List<Book> books;
        try (
                Connection connection = createConnection();
        ) {
            PreparedStatement booksStatement = connection.prepareStatement("select * from book where BOOK_TITLE like %?% or PUBLISHER like %?%");
            booksStatement.setString(1, keyWord);
            booksStatement.setString(2, keyWord);
            ResultSet rs = booksStatement.executeQuery();
            books = getBooks(connection, rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return books;
    }

    public List<Category> findAllCategories() {
        List<Category> categories = new ArrayList<>();
        try (
                Connection connection = createConnection();
                Statement categoriesStatement = connection.createStatement();
                ResultSet rs = categoriesStatement.executeQuery("select * from CATEGORY")
        ) {
            while (rs.next()) {
                categories.add(
                        new Category(
                                rs.getLong("ID"),
                                rs.getString("CATEGORY_DESCRIPTION ")));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return categories;
    }

    public void insert(Book book) {
        try (
                Connection connection = createConnection();
                Statement insertStatement = connection.createStatement();
        ) {
            insertStatement.executeUpdate(String.format("insert into Book (CATEGORY_ID, BOOK_TITLE, PUBLISHER) " +
                    "values (%s, '%s', '%s')", book.getCategoryId(), book.getBookTitle(), book.getPublisherName()));
            List<String> sqls = book.getAuthors().stream()
                    .map(author -> "insert into Author (BOOK_ID, FIRST_NAME, LAST_NAME) " +
                            String.format("values (%s, '%s', '%s')", author.getBookId(), author.getFirstName(), author.getLastName()))
                    .collect(Collectors.toList());
            for (String sql:sqls)
                insertStatement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void update(Book book) {
        try (
                Connection connection = createConnection();
                Statement updateStatement = connection.createStatement();
        ) {
            updateStatement.executeUpdate("update BOOK " +
                    String.format("set (CATEGORY_ID = %s, BOOK_TITLE = '%s', PUBLISHER = '%s') ", book.getCategoryId(), book.getBookTitle(), book.getPublisherName())+
                    String.format("where ID = %s", book.getId()));
            List<Pair<String,String>> sqls = book.getAuthors().stream()
                    .map(author -> {
                        MutablePair<String,String> pair = new MutablePair<>();
                        pair.setLeft("update AUTHOR " +
                                String.format("set (BOOK_ID = %s, FIRST_NAME = '%s', LAST_NAME = '%s') ", author.getBookId(), author.getFirstName(), author.getLastName()) +
                                String.format("where ID = %s", author.getId()));
                        pair.setRight("insert into Author (BOOK_ID, FIRST_NAME, LAST_NAME) " +
                                String.format("values (%s, '%s', '%s')", author.getBookId(), author.getFirstName(), author.getLastName()));
                        return pair;
                    })
                    .collect(Collectors.toList());
            for (Pair<String,String> sqlPair:sqls)
                if (updateStatement.executeUpdate(sqlPair.getLeft())==0)
                    updateStatement.executeUpdate(sqlPair.getRight());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(Long bookId) {
        try (
                Connection connection = createConnection();
                Statement deleteStatement = connection.createStatement();
        ) {
            deleteStatement.executeUpdate("delete from BOOK " +
                    String.format("where id = %s", bookId));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
