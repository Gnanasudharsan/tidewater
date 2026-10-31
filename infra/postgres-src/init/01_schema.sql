-- Source OLTP schema for the simulated e-commerce shop.
-- Runs once on first container start (docker-entrypoint-initdb.d).

create table customers (
    customer_id bigserial primary key,
    email text not null unique,
    full_name text not null,
    phone text,
    address_line text not null,
    city text not null,
    state text not null,
    postal_code text not null,
    country text not null default 'US',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table products (
    product_id bigserial primary key,
    sku text not null unique,
    name text not null,
    category text not null,
    unit_price numeric(10, 2) not null check (unit_price >= 0),
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table orders (
    order_id bigserial primary key,
    customer_id bigint not null references customers (customer_id) on delete cascade,
    status text not null default 'placed'
    check (status in ('placed', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded')),
    order_total numeric(12, 2) not null default 0 check (order_total >= 0),
    currency char(3) not null default 'USD',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table order_items (
    order_item_id bigserial primary key,
    order_id bigint not null references orders (order_id) on delete cascade,
    product_id bigint not null references products (product_id),
    quantity integer not null check (quantity > 0),
    unit_price numeric(10, 2) not null check (unit_price >= 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table payments (
    payment_id bigserial primary key,
    order_id bigint not null references orders (order_id) on delete cascade,
    amount numeric(12, 2) not null,
    method text not null check (method in ('card', 'paypal', 'gift_card', 'bank_transfer')),
    status text not null check (status in ('authorized', 'captured', 'refunded', 'failed')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index idx_orders_customer on orders (customer_id);
create index idx_orders_status on orders (status);
create index idx_order_items_order on order_items (order_id);
create index idx_payments_order on payments (order_id);

-- Keep updated_at honest without trusting the application.
create function set_updated_at() returns trigger
language plpgsql as $$
begin
    new.updated_at := now();
    return new;
end;
$$;

do $$
declare t text;
begin
    foreach t in array array['customers', 'products', 'orders', 'order_items', 'payments'] loop
        execute format(
            'create trigger trg_%1$s_updated_at before update on %1$s '
            'for each row execute function set_updated_at()', t
        );
        -- Full before-images on update/delete, so delete events carry every column.
        execute format('alter table %I replica identity full', t);
    end loop;
end;
$$;

-- Publication consumed by Debezium (pgoutput plugin).
create publication tidewater_pub for table customers, products, orders, order_items, payments;
