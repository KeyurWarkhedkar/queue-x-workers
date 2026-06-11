package com.keyur.queue_x_workers.Repositories;

import com.keyur.queue_x_workers.Entities.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface ProductRepository extends JpaRepository<Product, Long> {
    @Modifying
    @Transactional
    @Query("UPDATE Product p SET p.quantity = p.quantity - :quantity " +
            "WHERE p.id = :productId AND p.quantity >= :quantity")
    int atomicDecrement(@Param("productId") Long productId,
                       @Param("quantity") int quantity);

    @Modifying
    @Query("UPDATE Product p SET p.quantity = p.quantity + :quantity " +
    "WHERE p.id = :productId")
    void atomicIncrement(@Param("productId") Long productId,
                        @Param("quantity") int quantity);
}
