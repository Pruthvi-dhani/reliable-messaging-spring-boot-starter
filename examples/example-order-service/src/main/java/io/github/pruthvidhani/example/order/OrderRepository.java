package io.github.pruthvidhani.example.order;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Persists orders to the app's own {@code orders} table. */
@Repository
public class OrderRepository {

  private final JdbcTemplate jdbc;

  public OrderRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public void insert(String orderId, OrderRequest request) {
    jdbc.update(
        "insert into orders (order_id, customer_id, amount_pence, currency) values (?, ?, ?, ?)",
        orderId,
        request.customerId(),
        request.amountPence(),
        request.currency());
  }

  public int count() {
    Integer count = jdbc.queryForObject("select count(*) from orders", Integer.class);
    return count == null ? 0 : count;
  }
}
