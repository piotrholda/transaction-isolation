package com.piotrholda.transaction.isolation;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import static org.springframework.transaction.annotation.Isolation.READ_UNCOMMITTED;

@Repository
interface AccountRepository extends JpaRepository<Account, Integer> {

    @Query(value = "SELECT * FROM ACCOUNT a WHERE a.ID = :id", nativeQuery = true)
    Account getByIdNative(@Param("id") int id);

    @Modifying
    //@Transactional(isolation = READ_UNCOMMITTED)
    @Query(value = "UPDATE ACCOUNT SET BALANCE = :balance WHERE ID = :id", nativeQuery = true)
    void updateNative(@Param("id") int id, @Param("balance") int balance);
}
