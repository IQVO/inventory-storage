import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest/inventory-storage-api",
    },
    {
      type: "category",
      label: "Stock",
      link: {
        type: "doc",
        id: "api-reference/rest/stock",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/receive-stock",
          label: "Acknowledge inbound stock for a SKU",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/stow-stock",
          label: "Stow received stock into a bin",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Reservations",
      link: {
        type: "doc",
        id: "api-reference/rest/reservations",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-reservations-by-demand-ref",
          label: "Look up every reservation for a demand reference",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/reserve-stock",
          label: "Create a revocable reservation against usable inventory",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/revoke-reservation",
          label: "Revoke a reservation",
          className: "api-method delete",
        },
        {
          type: "doc",
          id: "api-reference/rest/confirm-pick",
          label: "Confirm a reservation's physical pick",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Inventory",
      link: {
        type: "doc",
        id: "api-reference/rest/inventory",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-usable-inventory",
          label: "Get usable inventory for a SKU",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Bins",
      link: {
        type: "doc",
        id: "api-reference/rest/bins",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/register-bin",
          label: "Register a bin, or converge an existing bin to a capacity",
          className: "api-method put",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-bin",
          label: "Get a bin's capacity and occupancy",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/run-cycle-count",
          label: "Record a physical cycle count for a bin",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Products",
      link: {
        type: "doc",
        id: "api-reference/rest/products",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/classify-product",
          label: "Retired - classify the SKU in product-master instead (410)",
          className: "menu__list-item--deprecated api-method put",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-product-classification",
          label: "Get a SKU's product classification (deprecated local copy)",
          className: "menu__list-item--deprecated api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Health",
      link: {
        type: "doc",
        id: "api-reference/rest/health",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-healthz",
          label: "Liveness probe",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-readyz",
          label: "Readiness probe",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Reports",
      link: {
        type: "doc",
        id: "api-reference/rest/reports",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-flow-accuracy-report",
          label: "Inventory Flow & Accuracy report (hourly)",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-flow-accuracy-freshness",
          label: "Freshness lag of the flow-accuracy read model",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
