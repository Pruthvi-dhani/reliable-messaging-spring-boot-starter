package io.github.pruthvidhani.example.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end HTTP semantics of the idempotent order endpoint: status codes, ProblemDetail bodies,
 * and replay behavior — through the real controller, aspect, and a real Postgres.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class OrderApiIT {

  @Container @ServiceConnection
  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private OrderService orderService;

  private static final String BODY =
      """
      {"customerId":"cust-1","amountPence":4999,"currency":"GBP"}
      """;

  @Test
  void placesAnOrderAndRetriesReplayTheSameConfirmation() throws Exception {
    int chargesBefore = orderService.timesCharged();

    MvcResult first =
        mvc.perform(
                post("/orders")
                    .header("Idempotency-Key", "checkout-attempt-1")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(BODY))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.orderId").isNotEmpty())
            .andExpect(jsonPath("$.status").value("CONFIRMED"))
            .andReturn();

    MvcResult retry =
        mvc.perform(
                post("/orders")
                    .header("Idempotency-Key", "checkout-attempt-1")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(BODY))
            .andExpect(status().isOk())
            .andReturn();

    JsonNode firstBody = objectMapper.readTree(first.getResponse().getContentAsString());
    JsonNode retryBody = objectMapper.readTree(retry.getResponse().getContentAsString());

    // Identical orderId proves the confirmation was replayed from cache, not re-created
    assertThat(retryBody.get("orderId")).isEqualTo(firstBody.get("orderId"));
    assertThat(orderService.timesCharged())
        .as("the customer must be charged exactly once")
        .isEqualTo(chargesBefore + 1);
  }

  @Test
  void sameKeyWithDifferentBodyIsRejectedWithExplanatory409() throws Exception {
    mvc.perform(
            post("/orders")
                .header("Idempotency-Key", "checkout-attempt-2")
                .contentType(MediaType.APPLICATION_JSON)
                .content(BODY))
        .andExpect(status().isOk());

    mvc.perform(
            post("/orders")
                .header("Idempotency-Key", "checkout-attempt-2")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"customerId\":\"cust-1\",\"amountPence\":9999,\"currency\":\"GBP\"}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.title").value("Idempotency key conflict"))
        .andExpect(jsonPath("$.detail").value(
            org.hamcrest.Matchers.containsString("different request body")));
  }

  @Test
  void missingIdempotencyKeyIsRejectedWithExplanatory400() throws Exception {
    mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(BODY))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.title").value("Idempotency key required"))
        .andExpect(jsonPath("$.detail").value(
            org.hamcrest.Matchers.containsString("Idempotency-Key header missing")));
  }

  @Test
  void differentKeysCreateIndependentOrders() throws Exception {
    MvcResult one =
        mvc.perform(
                post("/orders")
                    .header("Idempotency-Key", "checkout-attempt-3")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(BODY))
            .andExpect(status().isOk())
            .andReturn();

    MvcResult two =
        mvc.perform(
                post("/orders")
                    .header("Idempotency-Key", "checkout-attempt-4")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(BODY))
            .andExpect(status().isOk())
            .andReturn();

    JsonNode oneBody = objectMapper.readTree(one.getResponse().getContentAsString());
    JsonNode twoBody = objectMapper.readTree(two.getResponse().getContentAsString());
    assertThat(oneBody.get("orderId")).isNotEqualTo(twoBody.get("orderId"));
  }
}
